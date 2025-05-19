use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    // Compile the C wrapper
    cc::Build::new()
        .file("c-wrapper/librdma-wrapper.c")
        .include("/usr/include")
        .compile("ibverbs_wrappers");

    // Generate the Rust bindings
    let bindings = bindgen::Builder::default()
        .header("c-wrapper/librdma-wrapper.h")
        .header("/usr/include/infiniband/verbs.h")
        .header("/usr/include/rdma/rdma_cma.h")
        .clang_arg("-I/usr/include")
        .allowlist_type("ibv_access_flags")
        .allowlist_type("ibv_send_flags")
        .allowlist_type("rdma_cm_event_type")
        .allowlist_type("ibv_comp_channel")
        .allowlist_type("ibv_cq")
        .allowlist_type("ibv_mr")
        .allowlist_type("ibv_pd")
        .allowlist_type("ibv_qp")
        .allowlist_type("ibv_qp_attr")
        .allowlist_type("ibv_qp_cap")
        .allowlist_type("ibv_qp_init_attr")
        .allowlist_type("ibv_recv_wr")
        .allowlist_type("ibv_send_wr")
        .allowlist_type("ibv_send_wr__bindgen_ty_2")
        .allowlist_type("ibv_send_wr__bindgen_ty_2__bindgen_ty_1")
        .allowlist_type("ibv_sge")
        .allowlist_type("ibv_wc")
        .allowlist_type("ibv_wr_opcode")
        .allowlist_type("rdma_event_channel")
        .allowlist_type("rdma_cm_event")
        .allowlist_type("rdma_cm_id")
        .allowlist_type("rdma_conn_param")
        .allowlist_type("sa_family_t")
        .allowlist_type("sockaddr")
        .allowlist_type("sockaddr_in")
        .allowlist_type("in_addr")
        .constified_enum("ibv_access_flags")
        .constified_enum("ibv_send_flags")
        .constified_enum("rdma_cm_event_type")
        .allowlist_function("ibv_ack_cq_events")
        .allowlist_function("ibv_alloc_pd")
        .allowlist_function("ibv_ack_async_event")
        .allowlist_function("ibv_create_comp_channel")
        .allowlist_function("ibv_create_cq")
        .allowlist_function("ibv_dealloc_pd")
        .allowlist_function("ibv_query_qp")
        .allowlist_function("ibv_dereg_mr")
        .allowlist_function("ibv_destroy_comp_channel")
        .allowlist_function("ibv_get_async_event")
        .allowlist_function("ibv_destroy_cq")
        .allowlist_function("ibv_destroy_qp")
        .allowlist_function("ibv_free_device_list")
        .allowlist_function("ibv_get_cq_event")
        .allowlist_function("ibv_get_device_list")
        .allowlist_function("ibv_modify_qp")
        .allowlist_function("ibv_open_device")
        .allowlist_function("ibv_poll_cq_ex")
        .allowlist_function("ibv_post_recv_ex")
        .allowlist_function("ibv_req_notify_cq_ex")
        .allowlist_function("ibv_reg_mr")
        .allowlist_function("ibv_wc_status_str")
        .allowlist_function("ibv_wr_opcode_str")
        .allowlist_function("ibv_post_send_ex")
        .allowlist_function("rdma_accept")
        .allowlist_function("rdma_ack_cm_event")
        .allowlist_function("rdma_bind_addr")
        .allowlist_function("rdma_connect")
        .allowlist_function("rdma_create_event_channel")
        .allowlist_function("rdma_create_id")
        .allowlist_function("rdma_create_qp")
        .allowlist_function("rdma_destroy_event_channel")
        .allowlist_function("rdma_destroy_id")
        .allowlist_function("rdma_destroy_qp")
        .allowlist_function("rdma_disconnect")
        .allowlist_function("rdma_get_cm_event")
        .allowlist_function("rdma_listen")
        .allowlist_function("rdma_resolve_addr")
        .allowlist_function("rdma_resolve_route")
        .allowlist_var("AF_INET")
        .allowlist_var("RDMA_PS_TCP")
        .allowlist_var("UINT8_MAX")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let bindings_file = out_dir.join("bindings.rs");

    // Prepend #[allow(warnings)] to each item
    let generated = bindings.to_string();
    let mut output = String::new();

    for line in generated.lines() {
        // Add allow before items
        if line.starts_with("pub ")
            || line.starts_with("#[repr")
            || line.starts_with("extern ")
            || line.starts_with("impl ")
            || line.starts_with("unsafe impl")
        {
            output.push_str("#[allow(warnings)]\n");
        }
        output.push_str(line);
        output.push('\n');
    }

    fs::write(&bindings_file, output).expect("Couldn't write bindings!");

    println!("cargo:rustc-link-lib=ibverbs");
    println!("cargo:rustc-link-lib=rdmacm");
    println!("cargo:rustc-link-lib=static=ibverbs_wrappers");
    println!("cargo:warning=OUT_DIR is set to: {}", out_dir.display());
}
