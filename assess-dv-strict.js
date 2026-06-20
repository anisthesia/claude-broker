/**
 * Assess dv-* channels for strict schema enforcement readiness
 * Validates recent messages against their registered schemas
 */
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import Ajv from "ajv";
import "dotenv/config";

const BROKER_URL = process.env.BROKER_URL || "http://localhost:8080/mcp";
const SECRET = process.env.SHARED_SECRET || "";

const ajv = new Ajv();

async function connect(name) {
  const headers = SECRET ? { Authorization: `Bearer ${SECRET}` } : {};
  const transport = new StreamableHTTPClientTransport(new URL(BROKER_URL), { requestInit: { headers } });
  const client = new Client({ name, version: "1.0.0" });
  await client.connect(transport);
  return { client, transport };
}

function extractJsonFromSchemaResponse(text) {
  // Schema response format: header lines, then blank line, then JSON
  const parts = text.split('\n\n');
  if (parts.length >= 2) {
    try {
      return JSON.parse(parts.slice(1).join('\n\n'));
    } catch {}
  }
  try {
    return JSON.parse(text);
  } catch {}
  return null;
}

async function main() {
  const { client, transport } = await connect("assess-dv-strict");

  console.log(`[assess-dv-strict] broker: ${BROKER_URL}\n`);

  // Get list of channels
  const res = await client.callTool({ name: "list_channels", arguments: {} });
  const channelText = res.content[0].text;
  const channels = channelText.split('\n').map(line => line.split('\t')[0]).filter(ch => ch);
  const dvChannels = channels.filter(ch => ch.startsWith("dv-"));

  console.log(`Found ${dvChannels.length} dv-* channels\n`);

  const results = [];

  for (const channel of dvChannels.sort()) {
    // Get schema
    let schema = null;
    try {
      const res = await client.callTool({ name: "get_channel_schema", arguments: { channel } });
      const text = res.content[0]?.text;
      if (text) {
        schema = extractJsonFromSchemaResponse(text);
      }
    } catch {
      // No schema
    }

    if (!schema) {
      results.push({
        channel,
        schema_exists: false,
        message_count_checked: 0,
        violations: []
      });
      console.log(`⊘ ${channel}: no schema registered`);
      continue;
    }

    // Read messages
    let messages = [];
    try {
      const res = await client.callTool({ name: "read_messages", arguments: { channel, limit: 20 } });
      const text = res.content[0]?.text;
      if (text) {
        // Parse the text response (line-separated message format)
        messages = text.split('\n').filter(line => line.trim()).map(line => {
          const tabIndex = line.indexOf('\t');
          if (tabIndex === -1) return null;
          const id = parseInt(line.substring(0, tabIndex));
          const remainder = line.substring(tabIndex + 1);
          const parts = remainder.split('\t');
          if (parts.length < 3) return null;
          const timestamp = parts[0];
          const sender = parts[1];
          const content = parts.slice(2).join('\t');
          try {
            return { id, timestamp, sender, content: JSON.parse(content) };
          } catch {
            return { id, timestamp, sender, content: {} };
          }
        }).filter(m => m !== null);
      }
    } catch {
      // No messages
    }

    // Validate messages
    const violations = [];
    for (const msg of messages) {
      try {
        const validate = ajv.compile(schema);
        const valid = validate(msg.content);
        if (!valid) {
          violations.push({
            msg_id: msg.id,
            errors: validate.errors.map(e => ({
              instancePath: e.instancePath,
              keyword: e.keyword,
              message: e.message
            }))
          });
        }
      } catch (err) {
        violations.push({
          msg_id: msg.id,
          errors: [{ message: `Schema validation error: ${err.message}` }]
        });
      }
    }

    results.push({
      channel,
      schema_exists: true,
      message_count_checked: messages.length,
      violations: violations.length > 0 ? violations : []
    });

    const status = violations.length === 0 && messages.length > 0 ? "✓ PASS" : violations.length === 0 ? "⊘ NO_MSGS" : `✗ FAIL (${violations.length}/${messages.length} invalid)`;
    console.log(`${status.padEnd(30)} ${channel}`);
  }

  console.log("\n[Summary]\n");
  const safe = results.filter(r => r.schema_exists && r.violations.length === 0 && r.message_count_checked > 0);
  const unsafe = results.filter(r => r.schema_exists && r.violations.length > 0);
  const noMessages = results.filter(r => r.schema_exists && r.message_count_checked === 0);
  const noSchema = results.filter(r => !r.schema_exists);

  if (safe.length > 0) {
    console.log(`Safe to flip to strict (0 violations, messages exist): ${safe.length}`);
    safe.forEach(r => console.log(`  ✓ ${r.channel}`));
  } else {
    console.log(`Safe to flip to strict (0 violations, messages exist): 0`);
  }

  if (unsafe.length > 0) {
    console.log(`\nUnsafe to flip to strict (has violations): ${unsafe.length}`);
    unsafe.forEach(r => {
      console.log(`  ✗ ${r.channel} (${r.violations.length}/${r.message_count_checked} messages invalid)`);
      r.violations.slice(0, 2).forEach(v => {
        console.log(`     - msg_id ${v.msg_id}: ${v.errors[0]?.message}`);
      });
      if (r.violations.length > 2) console.log(`     - ... and ${r.violations.length - 2} more`);
    });
  }

  if (noMessages.length > 0) {
    console.log(`\nSchema exists but no recent messages: ${noMessages.length}`);
    noMessages.forEach(r => console.log(`  ⊘ ${r.channel}`));
  }

  if (noSchema.length > 0) {
    console.log(`\nNo schema registered: ${noSchema.length}`);
    noSchema.forEach(r => console.log(`  ⊘ ${r.channel}`));
  }

  console.log("\nConclusion:");
  if (safe.length > 0) {
    console.log(`${safe.length} channel(s) are ready for strict-flip: ${safe.map(r => r.channel).join(', ')}`);
  } else if (noMessages.length > 0) {
    console.log("All dv-* channels have schemas but no recent messages. Safe to flip once messages are validated.");
  } else {
    console.log("Cannot flip any dv-* channels to strict until violations are resolved.");
  }

  await transport.close();
  process.exit(0);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
