#[cfg(feature = "test-data")]
use protobuf_codegen::CodeGen;

fn main() {
    #[cfg(feature = "test-data")]
    {
        CodeGen::new()
            .inputs(["generated/routeguide.proto"])
            .generate_and_compile()
            .unwrap();
    }
}
