package main

// toolDefs returns the MCP tool catalog advertised via tools/list. Schemas use
// JSON Schema as required by the MCP spec.
func toolDefs() []map[string]interface{} {
	return []map[string]interface{}{
		{
			"name":        "spark_send",
			"description": "Send a message to the Spark bus. Use @Name to mention a specific agent, or @all to mention everyone. Mentioned agents are woken immediately. Does not wait.",
			"inputSchema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"message": map[string]interface{}{
						"type":        "string",
						"description": "The message text. Include @Name or @all to notify someone.",
					},
				},
				"required": []string{"message"},
			},
		},
		{
			"name":        "spark_poll",
			"description": "Read all new messages since this agent last read (cursor-based). Returns immediately without waiting.",
			"inputSchema": map[string]interface{}{
				"type":       "object",
				"properties": map[string]interface{}{},
			},
		},
		{
			"name":        "spark_wait",
			"description": "Block until this agent is @mentioned (or @all), then return new messages. Optional timeout in seconds; 0 or omitted waits indefinitely.",
			"inputSchema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"timeout": map[string]interface{}{
						"type":        "number",
						"description": "Seconds to wait before giving up. 0 or omitted means wait forever.",
					},
				},
			},
		},
		{
			"name":        "spark_go",
			"description": "Agent main-loop tool: optionally send a reply first, then block until the next @mention (or @all) and return the new messages. Set timeout to a number of seconds, or the string \"sleep\" for deep sleep (wait indefinitely, only woken by a mention).",
			"inputSchema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"reply": map[string]interface{}{
						"type":        "string",
						"description": "Optional message to send before waiting. Use @Name or @all to notify.",
					},
					"timeout": map[string]interface{}{
						"description": "Seconds to wait (number), or \"sleep\" for indefinite deep sleep.",
					},
				},
			},
		},
		{
			"name":        "spark_status",
			"description": "Show whether the writer daemon is running and each roster member's presence (online/sleeping/offline) and read cursor.",
			"inputSchema": map[string]interface{}{
				"type":       "object",
				"properties": map[string]interface{}{},
			},
		},
	}
}
