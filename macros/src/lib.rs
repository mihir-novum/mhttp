use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use quote::{format_ident, quote};
use syn::{
    ExprPath, Ident, ItemFn, LitStr, Path, Result, Token, Type, bracketed,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
};

fn resolve_crate(name: &str) -> Path {
    let found = crate_name(name);

    let path_str = match found {
        Ok(FoundCrate::Itself) => "crate".to_string(),
        Ok(FoundCrate::Name(name)) => {
            let ident = name.replace('-', "_");
            format!("::{}", ident)
        }
        Err(_) => format!("::{name}"),
    };

    syn::parse_str::<Path>(&path_str).unwrap_or_else(|_| {
        syn::parse_str::<Path>(&format!("::{name}"))
            .unwrap_or_else(|_| panic!("Failed to resolve `{name}` crate."))
    })
}

#[proc_macro_attribute]
pub fn route(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn: ItemFn = match syn::parse(item.clone()) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error().into(),
    };

    let parsed = match syn::parse::<RouteAttr>(attr) {
        Ok(v) => v,
        Err(e) => return e.to_compile_error().into(),
    };

    if input_fn.sig.asyncness.is_none() {
        return syn::Error::new_spanned(input_fn.sig.fn_token, "#[route] requires an `async fn`")
            .to_compile_error()
            .into();
    }
    if input_fn.sig.inputs.len() != 1 {
        return syn::Error::new_spanned(
            &input_fn.sig.inputs,
            "#[route] handler must take exactly one argument: `&mut HttpCall`",
        )
        .to_compile_error()
        .into();
    }
    let arg_ok = match input_fn.sig.inputs.first().unwrap() {
        syn::FnArg::Typed(pt) => match &*pt.ty {
            Type::Reference(r) => {
                r.mutability.is_some()
                    && matches!(&*r.elem, Type::Path(p) if p.path.segments.last().map(|s| s.ident.to_string()) == Some("HttpCall".to_string()))
            }
            _ => false,
        },
        _ => false,
    };
    if !arg_ok {
        return syn::Error::new_spanned(
            &input_fn.sig.inputs,
            "#[route] handler argument must be `&mut HttpCall`",
        )
        .to_compile_error()
        .into();
    }
    if let syn::ReturnType::Type(_, ty) = &input_fn.sig.output
        && !matches!(ty.as_ref(), Type::Tuple(t) if t.elems.is_empty())
    {
        return syn::Error::new_spanned(
            &input_fn.sig.output,
            "#[route] handler must return `()` (future output = ())",
        )
        .to_compile_error()
        .into();
    }

    let fn_name = &input_fn.sig.ident;
    let factory_name = format_ident!("__route_factory_{}", fn_name);

    if let Err(e) = validate_route_literal(&parsed.path) {
        return e.to_compile_error().into();
    }

    let route_lit = parsed.path.clone();
    let method_ts = parsed.method_tokens();

    let crate_name = resolve_crate("http");

    let mw_pushes: Vec<proc_macro2::TokenStream> = parsed
        .middleware
        .iter()
        .map(|mw: &ExprPath| {
            quote! {
                __mw.push(::std::sync::Arc::new(|call: &mut #crate_name::HttpCall| -> #crate_name::__future<'_, ()> {
                    ::std::boxed::Box::pin(#mw(call))
                }));
            }
        })
        .collect();

    let handler_init = quote! {
        {
            let __h: #crate_name::RouteHandler = ::std::sync::Arc::new(|call: &mut #crate_name::HttpCall| -> #crate_name::__future<'_, ()> {
                ::std::boxed::Box::pin(#fn_name(call))
            });
            __h
        }
    };

    let factory_fn = quote! {
        #[allow(non_snake_case)]
        fn #factory_name() -> #crate_name::RouteDefinition {
            let mut __mw: ::std::vec::Vec<#crate_name::MiddlewareHandler> = ::std::vec::Vec::new();
            #(#mw_pushes)*

            #crate_name::RouteDefinition::new(
                #method_ts,
                #route_lit,
                #handler_init,
                __mw
            ).expect(concat!("RouteDefinition::new failed for handler `", stringify!(#fn_name), "`"))
        }
    };

    let submit = quote! {
        #crate_name::__create_route_factory! {
            #crate_name::RouteFactory { factory: #factory_name }
        }
    };

    let expanded = quote! {
        #input_fn
        #factory_fn
        #submit
    };
    expanded.into()
}

struct RouteAttr {
    path: LitStr,
    method: Option<MethodSpec>,
    middleware: Vec<ExprPath>,
}

enum MethodSpec {
    Ident(Ident),
    Path(Path),
}

impl RouteAttr {
    fn method_tokens(&self) -> proc_macro2::TokenStream {
        let crate_name = resolve_crate("http");

        match &self.method {
            None => quote!(#crate_name::HttpMethod::GET),
            Some(MethodSpec::Ident(ident)) => quote!(#crate_name::HttpMethod::#ident),
            Some(MethodSpec::Path(p)) => quote!(#p),
        }
    }
}

impl Parse for RouteAttr {
    fn parse(input: ParseStream) -> Result<Self> {
        let path: LitStr = input.parse()?;

        let mut method: Option<MethodSpec> = None;
        let mut middleware: Vec<ExprPath> = Vec::new();

        while input.peek(Token![,]) {
            let _comma: Token![,] = input.parse()?;
            if input.is_empty() {
                break;
            }
            let key: Ident = input.parse()?;
            let _eq: Token![=] = input.parse()?;

            if key == "method" {
                let p: ExprPath = input.parse()?;
                let segs = &p.path.segments;
                method = if segs.len() == 1 {
                    Some(MethodSpec::Ident(segs.first().unwrap().ident.clone()))
                } else {
                    Some(MethodSpec::Path(p.path.clone()))
                };
            } else if key == "middleware" {
                if input.peek(syn::token::Bracket) {
                    let content;
                    bracketed!(content in input);
                    let elems: Punctuated<ExprPath, Token![,]> =
                        content.parse_terminated(ExprPath::parse, Token![,])?;
                    middleware.extend(elems.into_iter());
                } else {
                    let p: ExprPath = input.parse()?;
                    middleware.push(p);
                }
            } else {
                return Err(syn::Error::new(
                    key.span(),
                    "unknown #[route] argument; expected `method = ...` or `middleware = [...]`",
                ));
            }
        }

        Ok(Self {
            path,
            method,
            middleware,
        })
    }
}

fn validate_route_literal(path: &LitStr) -> Result<()> {
    let s = path.value();

    if !s.starts_with('/') {
        return Err(syn::Error::new(
            path.span(),
            "route must start with '/' (e.g., '/user/{id}')",
        ));
    }

    let bytes = s.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c == '{' {
            let start = i;
            i += 1;
            let name_start = i;
            while i < bytes.len() && (bytes[i] as char) != '}' {
                i += 1;
            }
            if i >= bytes.len() {
                return Err(syn::Error::new(
                    path.span(),
                    format!("unclosed '{{' at byte index {}", start),
                ));
            }
            let name = &s[name_start..i];
            if name.is_empty() {
                return Err(syn::Error::new(
                    path.span(),
                    format!("empty parameter name at byte index {}", start),
                ));
            }

            // ── reserved name check ──────────────────────────────────────
            if name.starts_with("__") && name.ends_with("__") {
                return Err(syn::Error::new(
                    path.span(),
                    format!(
                        "parameter name '{}' is reserved (names wrapped in `__` are for internal use only)",
                        name
                    ),
                ));
            }
            // ─────────────────────────────────────────────────────────────

            let mut chars = name.chars();
            match chars.next() {
                Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
                _ => {
                    return Err(syn::Error::new(
                        path.span(),
                        format!(
                            "invalid parameter name '{}': must start with [A-Za-z_]",
                            name
                        ),
                    ));
                }
            }
            if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
                return Err(syn::Error::new(
                    path.span(),
                    format!(
                        "invalid parameter name '{}': use [A-Za-z_][A-Za-z0-9_]*",
                        name
                    ),
                ));
            }
        } else if c == '}' {
            return Err(syn::Error::new(
                path.span(),
                format!("unexpected '}}' at byte index {}", i),
            ));
        }
        i += 1;
    }

    Ok(())
}

struct StaticFilesAttr {
    path: LitStr,
    dir: LitStr,
    index: Option<LitStr>,
    middleware: Vec<ExprPath>,
}

impl Parse for StaticFilesAttr {
    fn parse(input: ParseStream) -> Result<Self> {
        let path: LitStr = input.parse()?;
        let mut dir: Option<LitStr> = None;
        let mut index: Option<LitStr> = None;
        let mut middleware: Vec<ExprPath> = Vec::new();

        while input.peek(Token![,]) {
            let _: Token![,] = input.parse()?;
            if input.is_empty() {
                break;
            }

            let key: Ident = input.parse()?;
            let _: Token![=] = input.parse()?;

            if key == "dir" {
                dir = Some(input.parse()?);
            } else if key == "index" {
                index = Some(input.parse()?);
            } else if key == "middleware" {
                if input.peek(syn::token::Bracket) {
                    let content;
                    bracketed!(content in input);
                    let elems: Punctuated<ExprPath, Token![,]> =
                        content.parse_terminated(ExprPath::parse, Token![,])?;
                    middleware.extend(elems);
                } else {
                    middleware.push(input.parse()?);
                }
            } else {
                return Err(syn::Error::new(
                    key.span(),
                    "unknown argument; expected `dir`, `index`, or `middleware`",
                ));
            }
        }

        Ok(Self {
            path,
            dir: dir.ok_or_else(|| {
                syn::Error::new(proc_macro2::Span::call_site(), "`dir` is required")
            })?,
            index,
            middleware,
        })
    }
}

#[proc_macro_attribute]
pub fn static_files(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn: ItemFn = match syn::parse(item) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error().into(),
    };

    let parsed = match syn::parse::<StaticFilesAttr>(attr) {
        Ok(v) => v,
        Err(e) => return e.to_compile_error().into(),
    };

    // Validate route does NOT already contain {__path} — macro appends it
    let route_str = parsed.path.value();

    // 1. No params allowed in static_files routes — {__path__} is appended automatically
    if route_str.contains('{') || route_str.contains('}') {
        return syn::Error::new(
            parsed.path.span(),
            "#[static_files] route must not contain path parameters — `{__path__}` is appended automatically",
        ).to_compile_error().into();
    }

    // 2. Reject dir paths containing ".." — prevent path traversal at compile time
    let dir_str = parsed.dir.value();
    if dir_str.split('/').any(|seg| seg == "..") {
        return syn::Error::new(
            parsed.dir.span(),
            "`dir` must not contain `..` — use an absolute path or a path relative to the project root",
        ).to_compile_error().into();
    }

    // 3. existing check — {__path__} already present (now unreachable but keep for clarity)
    if route_str.contains("{__path__}") {
        return syn::Error::new(
            parsed.path.span(),
            "`{__path__}` is reserved and appended automatically by #[static_files] — remove it from the route",
        ).to_compile_error().into();
    }

    // Append {__path} to the route automatically
    // "/assets" -> "/assets/{__path}"
    // "/" -> "/{__path}"
    let final_route = if route_str.ends_with('/') {
        format!("{}{}", route_str, "{__path__}")
    } else {
        format!("{}/{}", route_str, "{__path__}")
    };
    let final_route_lit = syn::LitStr::new(&final_route, parsed.path.span());

    // Enforce same signature rules as #[route]
    if input_fn.sig.asyncness.is_none() {
        return syn::Error::new_spanned(
            input_fn.sig.fn_token,
            "#[static_files] requires an `async fn`",
        )
        .to_compile_error()
        .into();
    }

    let fn_name = &input_fn.sig.ident;
    let factory_name = format_ident!("__static_factory_{}", fn_name);
    let crate_path = resolve_crate("http");

    let route_lit = &final_route_lit;
    let dir_lit = &parsed.dir;

    let index_expr = match &parsed.index {
        Some(lit) => quote! { ::std::option::Option::Some(#lit) },
        None => quote! { ::std::option::Option::None },
    };

    let mw_pushes: Vec<proc_macro2::TokenStream> = parsed
        .middleware
        .iter()
        .map(|mw| {
            quote! {
                __mw.push(::std::sync::Arc::new(
                    |call: &mut #crate_path::HttpCall| -> #crate_path::__future<'_, ()> {
                        ::std::boxed::Box::pin(#mw(call))
                    }
                ));
            }
        })
        .collect();

    // The generated handler — ignores the original fn body entirely
    // and calls into our runtime serve_static function
    let expanded = quote! {
        // Keep the original fn so IDEs don't complain, but it's never called
        #[allow(dead_code)]
        #input_fn

        #[allow(non_snake_case)]
        fn #factory_name() -> #crate_path::RouteDefinition {
            async fn __serve(call: &mut #crate_path::HttpCall) {
                #crate_path::static_files::serve_static(
                    call,
                    #crate_path::static_files::StaticFileOptions {
                        dir: #dir_lit,
                        index: #index_expr,
                    },
                ).await;
            }

            let mut __mw: ::std::vec::Vec<#crate_path::MiddlewareHandler> = ::std::vec::Vec::new();
            #(#mw_pushes)*

            let __h: #crate_path::RouteHandler = ::std::sync::Arc::new(
                |call: &mut #crate_path::HttpCall| -> #crate_path::__future<'_, ()> {
                    ::std::boxed::Box::pin(__serve(call))
                }
            );

            #crate_path::RouteDefinition::new(
                #crate_path::HttpMethod::GET,
                #route_lit,
                __h,
                __mw,
            ).expect(concat!(
                "RouteDefinition::new failed for static handler `",
                stringify!(#fn_name),
                "`"
            ))
        }

        #crate_path::__create_route_factory! {
            #crate_path::RouteFactory { factory: #factory_name }
        }
    };

    expanded.into()
}
