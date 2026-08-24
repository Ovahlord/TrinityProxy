# TrinityProxy
A middleware service to counter denial of service attacks for TrinityCore based emulators.

This application acts as gateway for WoW client connections and implements several protection mechanisms to mitigate and disarm malicious DDos attacks.

# Connection Timeout
Each connection is monitored by a timeout window which will close a connection when no data has been coming for longer time periods.

# Rate Limiting
Each incoming connection uses a rate limited stream to avoid reading large chunks of data to memory at once. This ensures that each connection only gets a certain amount of bandwidth

# Per connection configurations
The service can configured to spawn as many listeners as desired with each of them being fully configurable.
