from dockersyncgraph.loop import main

# Register all events
import dockersyncgraph.musemtech

try:
    main()
except (SystemExit, KeyboardInterrupt):
    exit(0)
