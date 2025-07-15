#!/bin/bash

# 原始LOGO
read -r -d '' LOGO << 'EOF'

╔══════════════╗ ███████╗██╗   ██╗██████╗  ██████╗ 
║ ██  ██  ████ ║ ╚══███╔╝██║   ██║██╔══██╗██╔════╝ 
║ ███ █ ██████ ║   ███╔╝ ██║   ██║██████╔╝██║  ███╗
║ ███  ███████ ║  ███╔╝  ██║   ██║██╔═══╝ ██║   ██║
║ ██   ███████ ║ ███████╗╚████╔╝  ██║     ╚██████╔╝
╚══════════════╝ ╚══════╝ ╚═══╝   ╚═╝      ╚═════╝ 
EOF

# 预渲染函数
render_logo() {
    local WIDTH=$(echo "$LOGO" | head -1 | wc -m)
    local output=""

    while IFS= read -r line; do
        for (( i=0; i<${#line}; i++ )); do
            local ratio=$(echo "scale=3; $i / $WIDTH" | bc)
            local R=$(echo "255 - (255 * $ratio)" | bc | awk '{printf "%.0f", $1}')
            # 200 darker cyan, 255 very light cyan
            local G=$(echo "200 * $ratio" | bc | awk '{printf "%.0f", $1}')
            local B=$(echo "255 - (55 * $ratio)" | bc | awk '{printf "%.0f", $1}')
            output+="\033[38;2;${R};${G};${B}m${line:$i:1}"
        done
        output+="\033[0m\n"
    done <<< "$LOGO"

    echo -ne "$output"
}

render_logo > logo_output.txt
cat logo_output.txt