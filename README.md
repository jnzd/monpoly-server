# Monpoly Server

REST API endpoints:

- `/` - displays info page
- `/get-policy` - returns the current policy
- `/set-policy` - sets the policy
- `/get-signature` - returns the current signature
- `/set-signature` - sets the signature
- `/log-events` - requires a json array of events to send to the monitor, it forwards them to the monitor and logs timepoints in questdb, if they are in order and otherwise correct
