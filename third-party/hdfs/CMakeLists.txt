
add_custom_target(
    tgt_hdfs ALL
    COMMAND "./build.sh" ">" "./_build.log" "2>&1"
)
