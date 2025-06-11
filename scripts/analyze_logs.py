# To run this code you need to install the following dependencies:
# pip install google-genai redis

import base64
import os
import json
import redis
import sys
import math # Import math for ceil

# Add the parent directory to sys.path to allow importing config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google import genai
from google.genai import types

# Assuming config.py is in the parent directory or correctly imported
from config import config

def get_redis_logs():
    """Fetches logs from Redis."""
    try:
        # Use decode_responses=True to get strings directly
        redis_client = redis.from_url(config.REDIS_URL, decode_responses=True)
        # Fetch all logs up to MAX_LOG_ENTRIES
        # Fetching all logs first might still be memory intensive for very large logs,
        # but necessary to chunk them for the AI.
        # Consider fetching in batches from Redis if memory becomes an issue.
        log_entries = redis_client.lrange(config.API_LOG_KEY, 0, config.MAX_LOG_ENTRIES - 1)
        return log_entries
    except redis.exceptions.ConnectionError as e:
        print(f"Error connecting to Redis: {e}")
        return []
    except Exception as e:
        print(f"Error fetching logs from Redis: {e}")
        return []

def analyze_log_chunk(log_chunk_text, chunk_info, total_chunks):
    """Sends a single log chunk to the AI for analysis and returns the report."""
    client = genai.Client(
        api_key="AIzaSyAzSzm1L2ECUy_5Dm5hkMnvB-hozyMw5RI", # Use environment variable
    )

    model = "gemini-2.0-flash" # Use the model specified by the user

    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part.from_text(text=f"""# API Log Security Analysis Prompt - Log Chunk {chunk_info}/{total_chunks}

## Primary Objective
Perform a security analysis of this specific segment of API logs to identify potential threats, anomalies, and usage patterns. This is part of a larger analysis, and the findings from this chunk will be synthesized with others later.

## Analysis Framework (Focus on this chunk)

### 1. Security Threat Detection
- Authentication Anomalies
- Access Pattern Violations
- Authorization Bypasses
- Injection Attacks
- Rate Limiting Violations

### 2. Behavioral Analysis
- User Activity Profiling
- Endpoint Usage Patterns
- Error Response Analysis
- Payload Analysis

### 3. Statistical Metrics
- Endpoint Access Frequency
- Response Code Distribution
- Traffic Volume Analysis
- Client Distribution

### 4. Risk Assessment Criteria
- Critical Alerts
- Medium Risk
- Low Risk
- Baseline Activity

## Required Output Structure (for this chunk's analysis)

### Executive Summary (for this chunk)
Brief overview of key findings from this log segment.

### Critical Security Findings (in this chunk)
- List high-priority security incidents with severity ratings
- Include specific timestamps, IP addresses, and affected endpoints
- Provide recommended immediate actions for issues found *in this chunk*.

### Suspicious Activity Report (in this chunk)
- Detailed analysis of questionable patterns found *in this chunk*
- Context and potential implications
- Recommended follow-up investigations for issues found *in this chunk*.

### Endpoint Usage Statistics (for this chunk)
```
Endpoint Name | Request Count | % of Total | Avg Response Time | Error Rate
```

### Traffic Analysis (for this chunk)
- Peak usage periods (within this chunk)
- Geographic distribution anomalies (if data available)
- User agent analysis (for this chunk)

### Recommendations (for this chunk)
- Immediate security actions related to findings *in this chunk*
- Monitoring improvements based on findings *in this chunk*
- Policy adjustments suggested by findings *in this chunk*

## Context Notes
- This analysis covers a segment of the full API activity logs.
- It is part of a larger analysis that will combine findings from all chunks.
- Focus on providing detailed findings for this specific log segment.

## Log Data (this chunk)
```
{log_chunk_text}
```

Please provide a detailed analysis of the provided log chunk following the framework and output structure, focusing only on the data within this segment. Do NOT provide an overall summary or recommendations across all logs.
"""
                ),
            ],
        ),
    ]

    generate_content_config = types.GenerateContentConfig(
        response_mime_type="text/plain",
    )

    chunk_analysis = ""
    print(f"Analyzing chunk {chunk_info}/{total_chunks}...")
    try:
        for chunk in client.models.generate_content_stream(
            model=model,
            contents=contents,
            config=generate_content_config,
        ):
            chunk_analysis += chunk.text
        return chunk_analysis
    except Exception as e:
        print(f"Error analyzing chunk {chunk_info}/{total_chunks}: {e}")
        return f"\n\n--- Error Analyzing Log Chunk {chunk_info}/{total_chunks} ---\nError: {e}\n---"

def combine_analysis_reports(chunk_reports):
    """Sends individual chunk reports to the AI for combined analysis."""
    client = genai.Client(
        api_key="AIzaSyAzSzm1L2ECUy_5Dm5hkMnvB-hozyMw5RI", # Use environment variable
    )

    model = "gemini-2.0-flash" # Or a more capable model if needed for synthesis

    combined_input_text = ""
    for i, report in enumerate(chunk_reports):
        combined_input_text += f"\n\n--- Analysis Report for Log Chunk {i + 1} ---\n\n{report}"

    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part.from_text(text=f"""# API Log Security Analysis - Combined Report

## Primary Objective
Synthesize the provided individual analysis reports from multiple log chunks into a single, comprehensive security analysis report for the entire set of API logs.

## Instructions
- Review the executive summaries, critical findings, suspicious activities, traffic analyses, and recommendations from each chunk report.
- Consolidate and summarize the key findings across all chunks.
- Identify overarching patterns, persistent threats, and significant anomalies observed throughout the entire log timeframe.
- Generate a single, unified report covering all logs.

## Required Output Structure

### Executive Summary
Brief overview of the overall security posture and key findings from the entire log set.

### Critical Security Findings
- Consolidate high-priority security incidents observed across all chunks.
- Provide a summary of the most significant threats.
- Include consolidated recommendations for immediate actions.

### Suspicious Activity Report
- Consolidate and summarize suspicious patterns and behaviors observed across all chunks.
- Provide context and potential implications for the entire log set.
- Suggest consolidated follow-up investigations.

### Endpoint Usage Summary
- Summarize the overall endpoint access frequency and traffic volume trends based on the statistics provided in the chunk reports. You do not need to re-create the detailed tables unless you can accurately aggregate the data.

### Traffic Analysis Summary
- Summarize overall traffic patterns, peak periods (if discernible), geographic anomalies (if noted in chunks), and user agent distribution across all logs.

### Consolidated Recommendations
- Provide a unified set of recommendations for immediate security actions, monitoring improvements, and policy adjustments based on the findings from all log chunks.

## Individual Log Chunk Analysis Reports

```
{combined_input_text}
```

Please synthesize the above individual chunk reports into a single, comprehensive security analysis report following the required output structure.
"""
                ),
            ],
        ),
    ]

    generate_content_config = types.GenerateContentConfig(
        response_mime_type="text/plain",
    )

    final_report = ""
    print("Synthesizing combined report...")
    try:
        for chunk in client.models.generate_content_stream(
            model=model,
            contents=contents,
            config=generate_content_config,
        ):
            final_report += chunk.text
        return final_report
    except Exception as e:
        print(f"Error synthesizing combined report: {e}")
        return f"\n\n--- Error Synthesizing Combined Report ---\nError: {e}\n---"

def save_report(report_content, filename="log_analysis_report.txt", directory="reports"):
    """Saves the AI report to a file."""
    if not os.path.exists(directory):
        os.makedirs(directory)

    filepath = os.path.join(directory, filename)
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(report_content)
        print(f"Analysis report saved to {filepath}")
    except IOError as e:
        print(f"Error saving report to {filepath}: {e}")

if __name__ == "__main__":
    print("Fetching logs from Redis...")
    api_logs = get_redis_logs()

    if api_logs:
        print(f"Fetched {len(api_logs)} log entries. Analyzing in chunks...")

        chunk_size = 1000 # Adjust based on testing
        num_chunks = math.ceil(len(api_logs) / chunk_size)

        individual_reports = []
        for i in range(num_chunks):
            start_index = i * chunk_size
            end_index = min((i + 1) * chunk_size, len(api_logs))
            current_chunk_logs = api_logs[start_index:end_index]

            log_chunk_text = "\n".join(current_chunk_logs)

            chunk_report = analyze_log_chunk(log_chunk_text, i + 1, num_chunks)
            individual_reports.append(chunk_report)

        if individual_reports:
            print("Individual chunk analyses complete. Combining into a final report...")
            final_combined_report = combine_analysis_reports(individual_reports)

            if final_combined_report:
                save_report(final_combined_report)
            else:
                print("Failed to generate combined analysis report.")
        else:
            print("No individual chunk reports were generated.")
    else:
        print("No logs fetched from Redis or an error occurred.") 