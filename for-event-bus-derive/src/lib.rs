use proc_macro::TokenStream;
use quote::quote;
use syn::__private::TokenStream2;
use syn::{Fields, Item, ItemEnum, Type};

#[proc_macro_derive(Merge)]
pub fn merge_derive(input: TokenStream) -> TokenStream {
    match general_merge(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn general_merge(code: TokenStream2) -> Result<TokenStream2, syn::Error> {
    let item_enum = match syn::parse2::<Item>(code)? {
        Item::Enum(item_enum) => item_enum,
        item => {
            return Err(syn::Error::new_spanned(
                item,
                "derive(Merge) only supports enum",
            ));
        }
    };

    build_merge_tokens(item_enum)
}

fn build_merge_tokens(item_enum: ItemEnum) -> Result<TokenStream2, syn::Error> {
    let ident = item_enum.ident;
    let mut tokens = Vec::new();
    let mut type_ids = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for variant in item_enum.variants {
        let var_ident = variant.ident.clone();
        let field = match &variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => fields.unnamed.first().unwrap(),
            _ => {
                return Err(syn::Error::new_spanned(
                    &variant,
                    "each enum variant in derive(Merge) must be tuple-style with exactly one field, e.g. Close(CloseEvent)",
                ));
            }
        };

        let path = if let Type::Path(path) = &field.ty {
            path
        } else {
            return Err(syn::Error::new_spanned(
                &field.ty,
                "variant field must be a concrete type path, e.g. module::CloseEvent",
            ));
        };

        let type_key = quote!(#path).to_string();
        if !seen.insert(type_key.clone()) {
            return Err(syn::Error::new_spanned(
                &field.ty,
                format!("duplicate event type in derive(Merge): {type_key}"),
            ));
        }

        tokens.push(quote!(
            if let Ok(a_event) = payload.clone().downcast::<#path>() {
                Ok(Self::#var_ident(a_event.as_ref().clone()))
            }
        ));
        type_ids.push(quote!(
            (std::any::TypeId::of::<#path>(), stringify!(#path))
        ));
    }

    let end = quote!(
        impl for_event_bus::Merge for #ident {
            fn merge(event: for_event_bus::BusEvent) -> Result<Self, BusError>
            where
                Self: Sized,
            {
                let actual = event.type_name();
                let payload = event.as_any();
                #(#tokens)else* else {
                    Err(BusError::downcast_failed(stringify!(#ident), actual))
                }
            }

            fn subscribe_types() -> Vec<(std::any::TypeId, &'static str)> {
                vec![#(#type_ids),*]
            }
        }
    );
    Ok(end)
}

#[proc_macro_derive(Worker)]
pub fn worker_derive(input: TokenStream) -> TokenStream {
    match general_worker(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn general_worker(code: TokenStream2) -> Result<TokenStream2, syn::Error> {
    let ident = if let Ok(Item::Struct(item_enum)) = syn::parse2(code.clone()) {
        item_enum.ident
    } else if let Ok(Item::Enum(item_enum)) = syn::parse2(code) {
        item_enum.ident
    } else {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "derive(Worker) only supports enum or struct",
        ));
    };
    let name = ident.to_string();
    let end = quote!(
        impl for_event_bus::ToWorker for #ident {
            fn name() -> String {
                #name.to_string()
            }
        }
    );
    Ok(end)
}

#[proc_macro_derive(Event)]
pub fn event_derive(input: TokenStream) -> TokenStream {
    match general_event(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn general_event(code: TokenStream2) -> Result<TokenStream2, syn::Error> {
    let ident = if let Ok(Item::Struct(item_enum)) = syn::parse2(code.clone()) {
        item_enum.ident
    } else if let Ok(Item::Enum(item_enum)) = syn::parse2(code) {
        item_enum.ident
    } else {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "derive(Event) only supports enum or struct",
        ));
    };
    let name = ident.to_string();
    let end = quote!(
        impl for_event_bus::Event for #ident {
            fn name() -> &'static str {
                stringify!(#name)
            }
        }
    );
    Ok(end)
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    #[test]
    fn merge_rejects_named_fields_variant() {
        let code = quote! {
            enum Bad {
                A { value: i32 },
            }
        };
        let err = general_merge(code).unwrap_err().to_string();
        assert!(err.contains("exactly one field"));
    }

    #[test]
    fn merge_rejects_duplicate_inner_type() {
        let code = quote! {
            enum Bad {
                A(u32),
                B(u32),
            }
        };
        let err = general_merge(code).unwrap_err().to_string();
        assert!(err.contains("duplicate event type"));
    }
}
