# Execute API Quick Reference Guide

## Overview

The `execution_api.py` service provides endpoints for n8n to execute scheduled Python functions from the database.

## Endpoints

### 1. Execute Single Function

**Endpoint:** `POST /execute`

**Request:**
```json
{
  "function_name": "my_function",
  "api_key": "your-api-key"  // Optional if set in header
}
```

**Headers (Alternative):**
```
X-API-Key: your-api-key
```

**Response:**
```json
{
  "success": true,
  "function_name": "my_function",
  "output": "Function result here",
  "error": null,
  "execution_time_ms": 1234,
  "execution_id": 42,
  "message": "Execution completed successfully"
}
```

### 2. Execute Multiple Functions (Batch)

**Endpoint:** `POST /execute/batch`

**Request:**
```json
{
  "function_names": ["func1", "func2", "func3"],
  "api_key": "your-api-key"  // Optional if set in header
}
```

**Response:**
```json
{
  "total": 3,
  "successful": 2,
  "failed": 1,
  "results": [
    {
      "success": true,
      "function_name": "func1",
      "output": "Result 1",
      "execution_time_ms": 500,
      "execution_id": 43
    },
    {
      "success": true,
      "function_name": "func2",
      "output": "Result 2",
      "execution_time_ms": 750,
      "execution_id": 44
    },
    {
      "success": false,
      "function_name": "func3",
      "error": "Function execution failed",
      "execution_time_ms": 200,
      "execution_id": 45
    }
  ]
}
```

### 3. Get Ready Functions

**Endpoint:** `GET /functions/ready`

**Headers:**
```
X-API-Key: your-api-key
```

**Response:**
```json
{
  "count": 2,
  "functions": [
    {
      "function_name": "func1",
      "execution_interval": 24,
      "last_execution": "2025-01-15T10:00:00",
      "division": "B2H"
    },
    {
      "function_name": "func2",
      "execution_interval": 12,
      "last_execution": null,
      "division": "B2H"
    }
  ]
}
```

### 4. Health Check

**Endpoint:** `GET /health`

**Response:**
```json
{
  "status": "healthy",
  "docker": "available",
  "database": "available",
  "timestamp": 1705320000.0
}
```

## n8n Workflow Example

### Option 1: Query and Execute (Recommended)

```
1. Schedule Trigger (Every Hour)
   ↓
2. HTTP Request: GET /functions/ready
   - URL: http://your-api:8001/functions/ready
   - Headers: X-API-Key: {{$env.EXECUTE_API_KEY}}
   ↓
3. IF {{$json.count}} > 0:
   ↓
4. Extract function names from response
   ↓
5. HTTP Request: POST /execute/batch
   - URL: http://your-api:8001/execute/batch
   - Headers: X-API-Key: {{$env.EXECUTE_API_KEY}}
   - Body: {
       "function_names": {{$json.functions[*].function_name}}
     }
   ↓
6. Handle errors and send notifications
```

### Option 2: Direct Execution (If you track ready functions in n8n)

```
1. Schedule Trigger (Every Hour)
   ↓
2. Query database directly (SQL node)
   ↓
3. For each function:
   ↓
4. HTTP Request: POST /execute
   - URL: http://your-api:8001/execute
   - Headers: X-API-Key: {{$env.EXECUTE_API_KEY}}
   - Body: {
       "function_name": "{{$json.function_name}}"
     }
```

## Environment Variables

Add to your `.env` file:

```env
# Execute Service Configuration
EXECUTE_API_KEY=your-secure-random-key-here
EXECUTE_RATE_LIMIT_PER_MINUTE=60
EXECUTE_RATE_LIMIT_PER_HOUR=1000
```

## Running the Service

```bash
# Activate virtual environment
source venv/bin/activate

# Run the execution service
uvicorn execution_api:app --host 0.0.0.0 --port 8001 --reload
```

## Error Handling

### Common Errors

1. **401 Unauthorized**
   - Missing or invalid API key
   - Solution: Check `EXECUTE_API_KEY` in `.env` and n8n workflow

2. **404 Not Found**
   - Function not found in database
   - Solution: Verify function_name exists in database

3. **503 Service Unavailable**
   - Docker or database unavailable
   - Solution: Check service health endpoint, verify Docker is running

4. **500 Internal Server Error**
   - Unexpected error during execution
   - Solution: Check logs, verify function code is valid

## Execution History

All executions are automatically logged to `execution_history` table:

```sql
SELECT * FROM execution_history 
WHERE function_name = 'my_function' 
ORDER BY execution_timestamp DESC 
LIMIT 10;
```

This table includes:
- Success/failure status
- Output or error messages
- Execution time
- Timestamp

## Rate Limiting

Default limits:
- 60 requests per minute
- 1000 requests per hour

Adjust in `.env`:
```env
EXECUTE_RATE_LIMIT_PER_MINUTE=100
EXECUTE_RATE_LIMIT_PER_HOUR=5000
```

## Security Notes

1. **API Key**: Always use API key authentication in production
2. **HTTPS**: Use HTTPS in production (set up reverse proxy with SSL)
3. **Network**: Consider IP whitelisting for execute endpoint
4. **Secrets**: Store API keys in n8n environment variables, not in workflow code

## Testing

### Test with curl

```bash
# Health check
curl http://localhost:8001/health

# Execute function (with API key in header)
curl -X POST http://localhost:8001/execute \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d '{"function_name": "my_function"}'

# Get ready functions
curl http://localhost:8001/functions/ready \
  -H "X-API-Key: your-api-key"
```

## Monitoring

Check execution history for:
- Success rates
- Average execution times
- Error patterns
- Function usage

```sql
-- Success rate by function
SELECT 
  function_name,
  COUNT(*) as total_executions,
  SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful,
  AVG(execution_time_ms) as avg_time_ms
FROM execution_history
GROUP BY function_name
ORDER BY total_executions DESC;
```

