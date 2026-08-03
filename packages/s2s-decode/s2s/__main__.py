"""Allow ``python -m s2s`` to run the TCP terminator."""

from s2s.server import main

raise SystemExit(main())
