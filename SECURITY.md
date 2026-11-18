# Security policy

## Reporting a vulnerability

Report it privately through GitHub: the **Security** tab of this repository, then **Report a vulnerability**. Please do not open a public issue, discussion or pull request for it.

Include what you found and how to reproduce it. You will get an answer within 7 days, and a fix or a plan within 30 days for confirmed issues. You are credited in the advisory unless you ask not to be.

## Supported versions

Security fixes go into the latest release only.

## What counts

- A message from a peer or a client that makes a node panic, hang or allocate without bound.
- A sequence of crashes, partitions or message faults that loses an acknowledged write or returns a result that is not linearizable.

Out of scope: nodes that deliberately send false messages, and anything that follows from the port being reachable by someone who should not reach it. quorate assumes peers are honest and the network is trusted. It has no authentication and no encryption: whoever can connect can read, write and change the membership.
