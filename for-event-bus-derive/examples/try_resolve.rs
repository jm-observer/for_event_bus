use custom_utils::logger::log::debug;
use custom_utils::parse_file;
use quote::quote;
use syn::{Item, Type};

#[tokio::main]
async fn main() {
    custom_utils::logger::logger_stdout_debug();
    let codes = r#"
enum MergeEvent {
    AEvent(super::AEvent),
    Close(Close),
}"#;
    let token = parse_file(codes).await.unwrap();
    for item in token.items {
        if let Item::Enum(item_enum) = item {
            let ident = item_enum.ident;
            debug!("{:?}", ident);
            let mut tokens = Vec::new();
            for var in item_enum.variants {
                let var_ident = var.ident;
                debug!("{}", var_ident);
                for field in var.fields {
                    debug!("{:?}", field);
                    if let Type::Path(path) = field.ty {
                        let segments = path.path.segments;
                        debug!("{:?}", segments);

                        tokens.push(quote!(
                        if let Ok(a_event) = event.clone().downcast::<#segments>() {
                            Ok(Self::#var_ident(a_event.as_ref().clone()))
                        }));
                    }
                }
            }

            let end = quote!(
                impl Merge for #ident {
                    fn merge(event: for_event_bus::Event) -> Result<Self, BusError>
                    where
                        Self: Sized,
                    {
                        #(#tokens)else* else {
                            Err(BusError::DowncastErr)
                        }
                    }
                }
            );

            let formatted = prettyplease::unparse(&parse_file(end.to_string()).await.unwrap());
            debug!("{}", formatted);
        }
    }
}
