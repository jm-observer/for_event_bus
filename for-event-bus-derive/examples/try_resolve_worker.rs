use custom_utils::logger::log::debug;
use custom_utils::parse_file;
use quote::quote;
use syn::{Item};

#[tokio::main]
async fn main() {
    custom_utils::logger::logger_stdout_debug();
    let codes = r#"
struct MergeEvent {
}"#;
    let token = parse_file(codes).await.unwrap();
    for item in token.items {
        if let Item::Struct(item_enum) = item {
            let ident = item_enum.ident;
            let name = ident.to_string();
            let end = quote!(
                impl ToWorker for #ident {
                    fn name() -> String {
                        #name.to_string()
                    }
                }
            );
            let formatted = prettyplease::unparse(&parse_file(end.to_string()).await.unwrap());
            debug!("{}", formatted);
        }
    }
}
