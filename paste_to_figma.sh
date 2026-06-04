#!/bin/bash
# Run this from Terminal: bash ~/mercor-reporting/paste_to_figma.sh

SCREENSHOTS=(
  "Scale AI:/Users/chasegladden/mercor-reporting/competitor-screenshots/scale-ai.png"
  "Outlier AI:/Users/chasegladden/mercor-reporting/competitor-screenshots/outlier-ai.png"
  "Mindrift:/Users/chasegladden/mercor-reporting/competitor-screenshots/mindrift.png"
  "micro1:/Users/chasegladden/mercor-reporting/competitor-screenshots/micro1.png"
  "Turing:/Users/chasegladden/mercor-reporting/competitor-screenshots/turing.png"
  "DataAnnotation:/Users/chasegladden/mercor-reporting/competitor-screenshots/dataannotation.png"
  "Alignerr:/Users/chasegladden/mercor-reporting/competitor-screenshots/alignerr.png"
  "Invisible Technologies:/Users/chasegladden/mercor-reporting/competitor-screenshots/invisible-technologies.png"
  "Surge AI:/Users/chasegladden/mercor-reporting/competitor-screenshots/surge-ai.png"
)

# Switch to Figma tab and focus
osascript << 'ASEOF'
tell application "Google Chrome"
    activate
    repeat with w in windows
        repeat with t in tabs of w
            if URL of t contains "figma.com/board/0OuytEjHuUF05UDYmYLhO3" then
                set current tab of w to t
                set index of w to 1
                exit repeat
            end if
        end repeat
    end repeat
end tell
delay 2
tell application "System Events"
    tell process "Google Chrome"
        set frontmost to true
        click at {760, 500}
    end tell
end tell
delay 1
ASEOF

echo "Figma focused. Starting paste loop..."

for entry in "${SCREENSHOTS[@]}"; do
  name="${entry%%:*}"
  path="${entry#*:}"
  echo "Pasting: $name"

  # Copy image to clipboard
  osascript -e "set the clipboard to (read (POSIX file \"$path\") as «class PNGf»)"
  sleep 0.3

  # Activate Chrome and paste in one script
  osascript << ASEOF
tell application "Google Chrome"
    activate
end tell
delay 0.4
tell application "System Events"
    keystroke "v" using command down
end tell
delay 1.5
tell application "System Events"
    key code 53
end tell
delay 0.3
ASEOF

done

echo "All done! Check the Figma board."
