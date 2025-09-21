#!/bin/bash

log_file="/project/twitter-api-to-kinesis-stream-instance.log"

# Log message to log file
log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> "$log_file"
}

check_root_privileges() {
    if [ "$EUID" -ne 0 ]; then
        log_message "Script must be run as root. Exiting."
        exit 1
    fi
}

install_packages() {
    local packages=(python3 python3-pip wget unzip)
    log_message "Installing required packages: ${packages[*]}"
    yum update -y
    yum install -y "${packages[@]}"
}

install_python_libraries() {
    local requirements_file="requirements.txt"
    log_message "Installing Python libraries from $requirements_file"
    pip3 install -r "$requirements_file"
}

execute_python_script() {
    local kinesis_streamer_script="TweetListener2.py"
    local stream_name="natural-disaster-tweets"
    local interval=900
    
    log_message "Executing Python script"
    chmod +x "$kinesis_streamer_script"
    python3 "$kinesis_streamer_script" --stream_name "$stream_name" --interval "$interval" 
}

# Main function to run the entire script
main() {
    log_message "Starting the script"
    check_root_privileges
    install_packages
    install_python_libraries
    execute_python_script
    log_message "Script execution completed"
}

# Run the main function and redirect stdout and stderr to the log file
main >> "$log_file" 2>&1