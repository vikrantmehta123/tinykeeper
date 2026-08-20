# Task

Our error handling is basically if let Err(e) = ... { println!("Error") }. Production C++ code has thousands of lines dedicated to emitting Prometheus metrics, structured logging, handling network backpressure, rate-limiting malicious clients, and gracefully recovering from partial hardware disk failures.