 #!/usr/bin/env bash

alias python='uv run python'
alias python3='uv run python'
alias python312='uv run python'
alias python3.12='uv run python'

YELLOW='\033[1;33m'
NC='\033[0m' # No Color

 attempt=0
 maxAttempts=30

 while (( attempt < maxAttempts )); do
      echo -e "${YELLOW}========= Attempt $((attempt + 1)) of $maxAttempts ==========${NC}"
     if opencode-1142 --print-logs --log-level INFO -m opencode/big-pickle run "Read agents.md @agents.md, learn the workflow @WORKFLOW.md and start on the tasks @task.md" \
        | tee /dev/tty \
        | grep -q '<promise>COMPLETE-ALL-FINISHED</promise>'; then
          echo -e "${YELLOW}========= LOOPS ALL FINISHED ! ===========${NC}"
         break
     fi
     ((attempt++))
 done

