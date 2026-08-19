# Task

Clients don't just anonymously connect; they establish a "Session" with a specific timeout. We need a SessionTracker. If a client disconnects and the timeout expires before they reconnect, the server must actively tear down their session.
