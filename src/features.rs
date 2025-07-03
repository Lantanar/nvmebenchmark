use vroom::IdentifyControllerInfo;

fn ascii_to_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .trim_end_matches('\0')
        .trim()
        .to_string()
}


pub fn print_identify_controller_info(info: &IdentifyControllerInfo) {
    println!("NVMe Identify Controller Information:");
    println!("===================================");

    // --- Controller Capabilities and Features ---
    println!("\n--- Controller Capabilities and Features ---");
    let vid = info.vid;
    println!("PCI Vendor ID (VID)              : {:#06x}", vid);
    let ssvid = info.ssvid;
    println!("PCI Subsystem Vendor ID (SSVID)  : {:#06x}", ssvid);
    println!("Serial Number (SN)               : {}", ascii_to_string(&info.serial_number));
    println!("Model Number (MN)                : {}", ascii_to_string(&info.model_number));
    println!("Firmware Revision (FR)           : {}", ascii_to_string(&info.firmware_revision));
    if info.recommended_arbitration_burst > 0 {
        println!("Recommended Arbitration Burst    : 2^{} commands", info.recommended_arbitration_burst);
    }
    println!("IEEE OUI Identifier              : {:02x}-{:02x}-{:02x}", info.ieee_oui_identifier[0], info.ieee_oui_identifier[1], info.ieee_oui_identifier[2]);
    
    // --- Controller Multi-Path I/O and Namespace Sharing ---
    println!("\n--- Multi-Path I/O and Namespace Sharing ---");
    if (info.controller_mpath_ns_sharing & (1 << 0)) != 0 {
        println!("  - NVM subsystem has more than one port");
    }
    if (info.controller_mpath_ns_sharing & (1 << 1)) != 0 {
        println!("  - NVM subsystem has more than one controller");
    }
    if (info.controller_mpath_ns_sharing & (1 << 2)) != 0 {
        println!("  - Associated with an SR-IOV Virtual Function");
    }
     if (info.controller_mpath_ns_sharing & (1 << 3)) != 0 {
        println!("  - Asymmetric Namespace Access Reporting supported");
    }


    // --- Data Transfer and Versioning ---
    println!("\n--- Data Transfer and Versioning ---");
    if info.max_data_transfer_size == 0 {
        println!("Max Data Transfer Size (MDTS)    : No limit");
    } else {
        // The value is 2^n * CAP.MPSMIN. We'll just show the 2^n part.
        println!("Max Data Transfer Size (MDTS)    : 2^{} * (Min Page Size)", info.max_data_transfer_size);
    }
    let controller_id = info.controller_id;
    println!("Controller ID (CNTLID)           : {:#06x}", controller_id);
    let major = (info.version >> 16) & 0xFFFF;
    let minor = (info.version >> 8) & 0xFF;
    let tertiary = info.version & 0xFF;
    println!("Version (VER)                    : {}.{}.{}", major, minor, tertiary);


    // --- Optional Admin Command Support (OACS) ---
    println!("\n--- Optional Admin Command Support (OACS) ---");
    let oacs = info.optional_admin_command_support;
    if (oacs & (1 << 0)) != 0 { println!("  - Security Send/Receive supported"); }
    if (oacs & (1 << 1)) != 0 { println!("  - Format NVM supported"); }
    if (oacs & (1 << 2)) != 0 { println!("  - Firmware Commit/Image Download supported"); }
    if (oacs & (1 << 3)) != 0 { println!("  - Namespace Management supported"); }
    if (oacs & (1 << 4)) != 0 { println!("  - Device Self-test supported"); }
    if (oacs & (1 << 5)) != 0 { println!("  - Directives supported"); }
    if (oacs & (1 << 6)) != 0 { println!("  - NVMe-MI Send/Receive supported"); }
    if (oacs & (1 << 7)) != 0 { println!("  - Virtualization Management supported"); }
    if (oacs & (1 << 8)) != 0 { println!("  - Doorbell Buffer Config supported"); }
    if (oacs & (1 << 9)) != 0 { println!("  - Get LBA Status supported"); }
    if (oacs & (1 << 10)) != 0 { println!("  - Command and Feature Lockdown supported"); }


    // --- Firmware ---
    println!("\n--- Firmware ---");
    let fw = info.firmware_updates;
    println!("Firmware Slots                   : {}", (fw >> 1) & 0b111);
    if (fw & (1 << 0)) != 0 { println!("  - Slot 1 is Read-Only"); }
    if (fw & (1 << 4)) != 0 { println!("  - Activation without reset supported"); }

    // --- Queue Information ---
    println!("\n--- Queue Information ---");
    println!("Abort Command Limit              : {}", info.abort_command_limit + 1);
    println!("Async Event Request Limit        : {}", info.asynchronous_event_request_limit + 1);
    println!("Submission Queue Entry Size (SQES) : Min: 2^{}, Max: 2^{} bytes", info.submission_queue_entry_size & 0xF, (info.submission_queue_entry_size >> 4) & 0xF);
    println!("Completion Queue Entry Size (CQES) : Min: 2^{}, Max: 2^{} bytes", info.completion_queue_entry_size & 0xF, (info.completion_queue_entry_size >> 4) & 0xF);
    let num_namespaces = info.num_namespaces;
    println!("Number of Namespaces (NN)        : {}", num_namespaces);


    // --- NVM Command Set Attributes ---
    println!("\n--- Optional NVM Command Support (ONCS) ---");
    let oncs = info.optional_nvm_cmd_support;
    if (oncs & (1 << 0)) != 0 { println!("  - Compare supported"); }
    if (oncs & (1 << 1)) != 0 { println!("  - Write Uncorrectable supported"); }
    if (oncs & (1 << 2)) != 0 { println!("  - Dataset Management supported"); }
    if (oncs & (1 << 3)) != 0 { println!("  - Write Zeroes supported"); }
    if (oncs & (1 << 4)) != 0 { println!("  - Save/Select fields in Get/Set Features supported"); }
    if (oncs & (1 << 5)) != 0 { println!("  - Reservations supported"); }
    if (oncs & (1 << 6)) != 0 { println!("  - Timestamp feature supported"); }
    if (oncs & (1 << 7)) != 0 { println!("  - Verify command supported"); }
    if (oncs & (1 << 8)) != 0 { println!("  - Copy command supported"); }


    // --- Volatile Write Cache (VWC) ---
    println!("\n--- Volatile Write Cache (VWC) ---");
    if (info.volatile_write_cache & 1) != 0 {
        println!("  - Volatile Write Cache is present");
    } else {
        println!("  - No Volatile Write Cache");
    }
    
    // --- NVM Subsystem ---
    println!("\n--- NVM Subsystem ---");
    println!("NVM Subsystem Qualified Name (SUBNQN): {}", ascii_to_string(&info.subnqn));
}