// src/router.rs

use crate::request::HttpMethod;
use crate::route_definition::RouteDefinition;

/// A single segment of a route pattern after splitting on '/'.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    /// A literal segment e.g. "api", "users"
    Static(String),
    /// A named parameter e.g. {id}, {name}
    Param(String),
    /// The reserved catch-all wildcard {__path__} — matches zero or more segments
    CatchAll,
}

impl Segment {
    fn from_str(s: &str) -> Self {
        if s == "{__path__}" {
            Segment::CatchAll
        } else if s.starts_with('{') && s.ends_with('}') {
            Segment::Param(s[1..s.len() - 1].to_string())
        } else {
            Segment::Static(s.to_string())
        }
    }
}

/// A fully matched route with captured parameters.
pub(crate) struct Match<'a> {
    pub route: &'a RouteDefinition,
    pub params: Vec<(&'a str, &'a str)>, // (param_name, captured_value) - completely avoids String allocations
}

/// One node in the prefix tree.
/// Each node represents one path segment.
struct Node {
    /// Routes that terminate exactly at this node, keyed by HTTP method.
    handlers: Vec<RouteDefinition>,

    /// Children for exact static segment matches.
    /// Stored as sorted vec for binary search — faster than HashMap for small N.
    static_children: Vec<(String, Node)>,

    /// Child for a named parameter segment e.g. {id}.
    param_child: Option<(String, Box<Node>)>, // (param_name, child_node)

    /// Child for the catch-all {__path__} — always a leaf.
    catch_all: Option<RouteDefinition>,
}

impl Node {
    fn new() -> Self {
        Self {
            handlers: Vec::new(),
            static_children: Vec::new(),
            param_child: None,
            catch_all: None,
        }
    }

    /// Insert a route into the tree given its remaining segments.
    fn insert(&mut self, segments: &[Segment], route: RouteDefinition) {
        if segments.is_empty() {
            // This node is the terminal for this route
            self.handlers.push(route);
            return;
        }

        match &segments[0] {
            Segment::Static(s) => {
                match self
                    .static_children
                    .binary_search_by(|(k, _)| k.as_str().cmp(s.as_str()))
                {
                    Ok(idx) => {
                        self.static_children[idx].1.insert(&segments[1..], route);
                    }
                    Err(idx) => {
                        let mut child = Node::new();
                        child.insert(&segments[1..], route);
                        self.static_children.insert(idx, (s.clone(), child));
                    }
                }
            }

            Segment::Param(name) => {
                if let Some((_, child)) = &mut self.param_child {
                    child.insert(&segments[1..], route);
                } else {
                    let mut child = Box::new(Node::new());
                    child.insert(&segments[1..], route);
                    self.param_child = Some((name.clone(), child));
                }
            }

            Segment::CatchAll => {
                // Catch-all is always a terminal — no further segments
                self.catch_all = Some(route);
            }
        }
    }

    /// Walk the tree matching path segments against this node's children.
    /// Returns the matched RouteDefinition and captured params if found.
    fn find<'a>(
        &'a self,
        path: &'a str,
        method: &HttpMethod,
        params: &mut Vec<(&'a str, &'a str)>,
    ) -> Option<&'a RouteDefinition> {
        let (seg, rest) = match next_segment(path) {
            Some(t) => t,
            None => {
                // 1. Look for an exact match handler
                if let Some(found) = self.handlers.iter().find(|r| &r.method == method) {
                    return Some(found);
                }

                // 2. CRITICAL FIX: Check catch-all for empty tails!
                // (e.g. `/assets` successfully matches `/assets/{__path__}` with an empty string)
                if let Some(route) = &self.catch_all {
                    if &route.method == method {
                        params.push(("__path__", ""));
                        return Some(route);
                    }
                }
                return None;
            }
        };

        // 1. Try exact static match first — most specific, highest priority
        if let Ok(idx) = self
            .static_children
            .binary_search_by(|(k, _)| k.as_str().cmp(seg))
        {
            if let Some(found) = self.static_children[idx].1.find(rest, method, params) {
                return Some(found);
            }
        }

        // 2. Try param match — captures the segment value
        if let Some((param_name, child)) = &self.param_child {
            let saved_len = params.len();
            params.push((param_name.as_str(), seg));

            if let Some(found) = child.find(rest, method, params) {
                return Some(found);
            }

            // Backtrack — this branch didn't match
            params.truncate(saved_len);
        }

        // 3. Try catch-all — matches this segment and everything remaining
        if let Some(route) = &self.catch_all {
            if &route.method == method {
                // Capture everything remaining starting from current segment (Zero Allocations!)
                let tail = path.trim_start_matches('/');
                params.push(("__path__", tail));
                return Some(route);
            }
        }

        None
    }
}

/// Helper function to extract the next non-empty segment and the remaining path string slice.
fn next_segment(path: &str) -> Option<(&str, &str)> {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    match trimmed.find('/') {
        Some(idx) => Some((&trimmed[..idx], &trimmed[idx..])),
        None => Some((trimmed, "")),
    }
}

/// The public router — wraps the root node.
/// Built once at server startup, read-only at runtime.
pub(crate) struct Router {
    root: Node,
}

impl Router {
    /// Build the router from a list of route definitions.
    /// Called once in HttpServerBuilder::build().
    pub fn build(routes: Vec<RouteDefinition>) -> Self {
        let mut root = Node::new();

        for route in routes {
            let segments = Self::parse_segments(&route.pattern);
            root.insert(&segments, route);
        }

        Self { root }
    }

    /// Find a matching route for the given method and path.
    /// Strips query string before matching.
    /// Returns None if no route matches.
    pub fn find<'a>(&'a self, method: &HttpMethod, path: &'a str) -> Option<Match<'a>> {
        // Strip query string — match on path only
        let path = match path.find('?') {
            Some(i) => &path[..i],
            None => path,
        };

        // Instantiates with capacity zero - 0 Heap Allocations for purely static routes!
        let mut params = Vec::new();

        self.root
            .find(path, method, &mut params)
            .map(|route| Match { route, params })
    }

    fn parse_segments(pattern: &str) -> Vec<Segment> {
        pattern
            .split('/')
            .filter(|s| !s.is_empty())
            .map(Segment::from_str)
            .collect()
    }
}