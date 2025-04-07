#[macro_export]
macro_rules! debug_println {
    // Enabled version
    ($($arg:tt)*) => {
        {
            #[cfg(any(debug_mode, debug_mode_verbose))]
            {
                println!($($arg)*);
            }

            #[cfg(not(any(debug_mode, debug_mode_verbose)))]
            {
                // do nothing
            }
        }
    };
}

#[macro_export]
macro_rules! debug_println_verbose {
    ($($arg:tt)*) => {
        {
            #[cfg(debug_mode_verbose)]
            {
                println!($($arg)*);
            }

            #[cfg(not(debug_mode_verbose))]
            {
                // do nothing
            }
        }
    };
}
