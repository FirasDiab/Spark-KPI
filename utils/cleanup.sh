#!/bin/bash

# cleanup.sh - Comprehensive Spark temporary files cleanup script
# Runs every 5 minutes (300 seconds)

LOG_FILE="/opt/spark/work/cleanup.log"
ERROR_LOG="/opt/spark/work/cleanup_errors.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" | tee -a "$ERROR_LOG"
}

# CLEAN UP STDERR/STDOUT FILES IN SPARK WORK DIRECTORY
cleanup_stderr_stdout() {
    find /opt/spark/work -type f \( -name 'stderr*' -o -name 'stdout*' \) -delete -print | while read -r f; do
        log "Deleted $f"
    done
    [[ ${PIPESTATUS[0]} -eq 0 ]] && log "Cleaned up stderr/stdout files in /opt/spark/work"
}

# CLEAN UP OLD FILES IN /TMP (OLDER THAN 4 HOURS)
cleanup_old_tmp_files() {
    find /tmp -type f -mmin +240 -delete -print | while read -r f; do
        log "Deleted old tmp file: $f"
    done
    log "Cleaned up /tmp files older than 4 hours"
}

# MAIN FUNCTION: CLEAN UP ALL STALE SPARK LOCAL DIRECTORIES
cleanup_spark_local_dirs() {
    local spark_local_base="/tmp"

    # FIND ALL SPARK-* DIRECTORIES UNDER /TMP (ONE PER USER/APP)
    find "$spark_local_base" -maxdepth 1 -type d -name 'spark-*' | while read -r spark_user_dir; do
        log "Processing Spark local directory: $spark_user_dir"

        # FIND ALL EXECUTOR-* SUBDIRECTORIES
        find "$spark_user_dir" -type d -name 'executor-*' | while read -r exec_dir; do
            log "  Cleaning executor directory: $exec_dir"

            # --- CLEAN BLOCKMGR-* DIRECTORIES (DELETE CONTENTS IF NOT ACCESSED IN LAST HOUR) ---
            find "$exec_dir" -maxdepth 1 -type d -name 'blockmgr-*' | while read -r blockmgr_dir; do
                if [[ ! -d "$blockmgr_dir" ]]; then
                    continue
                fi

                # GET THE MOST RECENTLY ACCESSED FILE IN THIS BLOCKMGR DIRECTORY
                latest_file=$(find "$blockmgr_dir" -type f -printf '%A@ %p\n' 2>/dev/null | sort -nr | head -n 1 | cut -d' ' -f2-)

                if [[ -z "$latest_file" ]]; then
                    log "    No files in $blockmgr_dir - removing entire directory"
                    continue
                fi

                last_access=$(stat -c %Y "$latest_file")
                current_time=$(date +%s)
                age_seconds=$(( current_time - last_access ))

                if (( age_seconds > 7200 )) && ((age_seconds <= 9000)); then
                    log "    $blockmgr_dir inactive for $age_seconds seconds (>4h) - deleting contents"
                    rm -rf "$blockmgr_dir"/* && log "    Cleared contents of $blockmgr_dir"
                else
                    log "    $blockmgr_dir recently accessed ($age_seconds seconds ago) - skipping"
                fi
            done

            if [[ -z "$(find "$exec_dir" -mindepth 1)" ]] && [[ $(find "$exec_dir" -maxdepth 0 -mmin +720) ]]; then
                log "  Removing empty and old executor dir: $exec_dir"
                rm -rf "$exec_dir"
            fi
        done
    done

    log "Finished cleaning Spark local directories under $spark_local_base"
}

# -------------------------------
# MAIN CLEANUP ROUTINE
# -------------------------------
run_cleanup() {
    log "=== Starting cleanup run ==="

    cleanup_stderr_stdout
    cleanup_old_tmp_files
    cleanup_spark_local_dirs

    log "=== Cleanup run completed ===\n"
}

# RUN ONCE IMMEDIATELY
run_cleanup

# THEN RUN EVERY 5 MINUTES
while true; do
    sleep 300
    run_cleanup
done