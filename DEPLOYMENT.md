# Multi-Server Deployment Guide (No Docker Required)

## Quick Start

### Monolith Mode (Recommended for Development)
Run everything in a single process - **this is the simplest approach**:

```bash
# Set environment (optional - monolith is default)
export DEPLOYMENT_MODE=monolith

# Run server
python server.py
```

The server runs on `http://localhost:8001` with all agents in-process. **This is what you're already doing!**

### Distributed Mode (Multiple Servers)

Run each agent as a separate server for independent scaling and isolation.

#### Step 1: Start Marketing Agent Server

Open a new terminal and run:

```bash
cd /Users/ajaydas/Downloads/RebuildMA-main

# Set environment
export DEPLOYMENT_MODE=distributed
export MARKETING_PORT=8002

# Run marketing agent
python agents/marketing/server.py
```

You should see:
```
🚀 Starting Marketing Agent Server on port 8002
✅ Marketing Agent Server initialized
```

#### Step 2: Start PacePal Orchestrator

Open another terminal and run:

```bash
cd /Users/ajaydas/Downloads/RebuildMA-main

# Set environment
export DEPLOYMENT_MODE=distributed
export MARKETING_AGENT_URL=http://localhost:8002
export PACEPAL_PORT=8001

# Run PacePal orchestrator
python pacepal/server.py
```

You should see:
```
🚀 Starting PacePal Server on port 8001
📝 Deployment mode: distributed
🌐 Distributed mode - MCPs handled by individual agents
```

#### Step 3: Connect Your Client

Your client connects to PacePal at `http://localhost:8001/ws/chat` (same as before).

## Environment Variables

### Required
- `OPENAI_API_KEY` - Your OpenAI API key (add to `.env` file)

### Deployment Mode
- `DEPLOYMENT_MODE` - Either `monolith` (default) or `distributed`

### Salesforce Credentials (Add to `.env` file)
```bash
# Agent Org (for MCP registry)
AGENT_SALESFORCE_USERNAME=your_agent_username@example.com
AGENT_SALESFORCE_PASSWORD=your_agent_password
AGENT_SALESFORCE_SECURITY_TOKEN=your_agent_security_token
AGENT_SALESFORCE_DOMAIN=login

# Marketing Org (for marketing operations)
MARKETING_SALESFORCE_USERNAME=your_marketing_username@example.com
MARKETING_SALESFORCE_PASSWORD=your_marketing_password
MARKETING_SALESFORCE_SECURITY_TOKEN=your_marketing_security_token
MARKETING_SALESFORCE_DOMAIN=login
```

### Agent URLs (Distributed Mode Only)
- `MARKETING_AGENT_URL` - URL of marketing agent (e.g., `http://localhost:8002`)
- `INTEGRATION_AGENT_URL` - URL of integration agent (default: `local`)
- `IO_AGENT_URL` - URL of IO agent (default: `local`)

### Ports
- `PACEPAL_PORT` - PacePal server port (default: `8001`)
- `MARKETING_PORT` - Marketing agent port (default: `8002`)

## Testing

### Test Monolith Mode
```bash
# Start server
python server.py

# In another terminal, test with your client
python test_client.py
```

### Test Distributed Mode

**Terminal 1:**
```bash
python agents/marketing/server.py
```

**Terminal 2:**
```bash
export MARKETING_AGENT_URL=http://localhost:8002
python pacepal/server.py
```

**Terminal 3:**
```bash
# Test health checks
curl http://localhost:8001/health
curl http://localhost:8002/health

# Test with client
python test_client.py
```

## Architecture Comparison

### Monolith Mode (Single Process)
```
Client → server.py (port 8001)
           ↓
        PacePal Orchestrator
           ↓
        Marketing Agent (in-process)
           ↓
        MCPs (Brevo, Linkly, Salesforce)
```

**Pros:**
- ✅ Simple - one command to start
- ✅ Easy debugging
- ✅ No network overhead
- ✅ Perfect for development

**Cons:**
- ❌ Can't scale agents independently
- ❌ One agent crash affects all

### Distributed Mode (Multiple Processes)
```
Client → pacepal/server.py (port 8001)
           ↓ (HTTP)
        agents/marketing/server.py (port 8002)
           ↓
        MCPs (Brevo, Linkly, Salesforce)
```

**Pros:**
- ✅ Scale agents independently
- ✅ Isolate failures
- ✅ Deploy agents separately
- ✅ Different teams can own different agents

**Cons:**
- ❌ More complex setup
- ❌ Network latency
- ❌ Need to manage multiple processes

## Process Management (Production)

For production, you'll want to keep servers running automatically. Here are some options:

### Option 1: tmux/screen (Simple)

```bash
# Start marketing agent in background
tmux new -d -s marketing "python agents/marketing/server.py"

# Start PacePal in background
tmux new -d -s pacepal "MARKETING_AGENT_URL=http://localhost:8002 python pacepal/server.py"

# List sessions
tmux ls

# Attach to a session
tmux attach -t marketing
```

### Option 2: systemd (Linux)

Create `/etc/systemd/system/marketing-agent.service`:
```ini
[Unit]
Description=Marketing Agent Server
After=network.target

[Service]
Type=simple
User=youruser
WorkingDirectory=/path/to/RebuildMA-main
Environment="DEPLOYMENT_MODE=distributed"
Environment="MARKETING_PORT=8002"
ExecStart=/usr/bin/python3 agents/marketing/server.py
Restart=always

[Install]
WantedBy=multi-user.target
```

Then:
```bash
sudo systemctl enable marketing-agent
sudo systemctl start marketing-agent
sudo systemctl status marketing-agent
```

### Option 3: supervisord (Cross-platform)

Install: `pip install supervisor`

Create `supervisord.conf`:
```ini
[program:marketing-agent]
command=python agents/marketing/server.py
directory=/path/to/RebuildMA-main
environment=DEPLOYMENT_MODE="distributed",MARKETING_PORT="8002"
autostart=true
autorestart=true
stdout_logfile=/var/log/marketing-agent.log

[program:pacepal]
command=python pacepal/server.py
directory=/path/to/RebuildMA-main
environment=DEPLOYMENT_MODE="distributed",MARKETING_AGENT_URL="http://localhost:8002"
autostart=true
autorestart=true
stdout_logfile=/var/log/pacepal.log
```

Run: `supervisord -c supervisord.conf`

## Adding New Agents

1. Create agent directory: `agents/your_agent/`
2. Create `agents/your_agent/graph.py` with agent logic
3. Create `agents/your_agent/server.py` (copy from marketing agent)
4. Update `shared/config.py` to add agent URL and port
5. Update `pacepal/orchestrator.py` to add routing logic
6. Start the new agent server in a new terminal

## Troubleshooting

### "Agent not configured for remote access"
- Ensure `MARKETING_AGENT_URL` is set when running in distributed mode
- Example: `export MARKETING_AGENT_URL=http://localhost:8002`

### Connection refused
- Verify the marketing agent server is running
- Check the port is correct (default: 8002)
- Ensure no firewall is blocking the port

### Port already in use
```bash
# Find what's using the port
lsof -i :8001
lsof -i :8002

# Kill the process if needed
kill -9 <PID>
```

### Import errors
```bash
# Ensure you're in the project directory
cd /Users/ajaydas/Downloads/RebuildMA-main

# Ensure dependencies are installed
pip install -r requirements.txt
```

### Can't find modules
Make sure you're running from the project root directory:
```bash
cd /Users/ajaydas/Downloads/RebuildMA-main
python pacepal/server.py  # ✅ Correct
```

## Logs

- Monolith mode: `agent.log`
- PacePal server: `pacepal.log`
- Marketing agent: `marketing_agent.log`

View logs in real-time:
```bash
tail -f agent.log
tail -f pacepal.log
tail -f marketing_agent.log
```

## Recommendations

**For Development:**
- Use **monolith mode** (`python server.py`)
- Simple, fast, easy to debug

**For Production:**
- Use **distributed mode** if you need:
  - Independent scaling of agents
  - Fault isolation
  - Separate deployments
- Use a process manager (systemd, supervisord, or pm2)
- Set up monitoring and alerting

**For Testing:**
- Test both modes to ensure everything works
- Monolith mode should work exactly as before
- Distributed mode adds HTTP communication between agents

## Summary

You have two deployment options:

1. **Monolith** (default): `python server.py` - Everything in one process
2. **Distributed**: Run `agents/marketing/server.py` and `pacepal/server.py` separately

Both work without Docker. Choose based on your needs:
- Development? → Monolith
- Production with scaling needs? → Distributed

The architecture supports both seamlessly!
