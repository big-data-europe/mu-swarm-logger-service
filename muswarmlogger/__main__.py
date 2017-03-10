from muswarmlogger.loop import main

# Register all events
import muswarmlogger.musemtech

try:
    main()
except (SystemExit, KeyboardInterrupt):
    exit(0)
