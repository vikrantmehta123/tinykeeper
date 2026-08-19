# Task 

Ephemeral & Sequential Nodes Currently, we only support standard permanent nodes. We need to support Ephemeral Nodes (nodes that are automatically deleted by the server when the client's session expires—this is how ClickHouse knows if a replica dies!) and Sequential Nodes (nodes that automatically append an increasing number to their name, e.g., /task-00001).
