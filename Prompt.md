You are a senior staff engineer and system design educator. I am providing you with a 21-day system design curriculum. Your task is to generate a detailed, reference-grade markdown file for Day {N} only.

## Context
- The learner is a software engineer targeting senior/staff-level system design proficiency
- All days up to Day {N-1} are fully completed — assume deep familiarity with every concept, tool, and trade-off covered in those days
- Do not re-explain foundational concepts already covered in prior days; instead, reference them by name and build on them
- Tone: peer-to-peer, no hand-holding, technically precise

## The curriculum (reference for prior knowledge and day content)
{PASTE THE FULL CONTENTS OF system_design_21_day_curriculum.md HERE}

## Output instructions
Generate a markdown file for Day {N}: {DAY TITLE} with the following sections:

### 1. Day summary
One sharp paragraph — what this problem is really testing, and what mental model shift it requires over prior days.

### 2. Pre-read checklist
Bullet list of specific concepts from Days 1–{N-1} the learner must be fluent in before starting today. Name the concept and the day it was covered. No filler — only genuine prerequisites.

### 3. The problem, stated precisely
Restate the problem with exact numbers: QPS, storage, latency SLAs, replication factors, scale targets. Make it interview-ready. Include both functional and non-functional requirements.

### 4. Capacity estimation
Full back-of-envelope calculation:
- Write QPS and read QPS
- Storage per day, per year
- Bandwidth (ingress and egress)
- Memory requirements for caching layer
- Show your math. Round to clean numbers.

### 5. Core approaches
For every approach listed in the curriculum for this day:
- What problem it solves specifically
- How it works at a technical level (data structures, algorithms, protocols)
- Time/space complexity where applicable
- Failure modes and edge cases
- When NOT to use it
- How it interacts with or builds on concepts from prior days

### 6. System architecture walkthrough
Describe the full system design in prose — component by component — as you would narrate it to an interviewer. Cover:
- The write path (step by step)
- The read path (step by step)
- How failures are handled at each layer
- Where bottlenecks appear as scale increases and how each is addressed

### 7. Data model
Define the key entities, their fields, data types, and storage choices. For each entity explain: why this storage engine, what the access patterns are, and how it scales.

### 8. Interview questions with model answers
5 questions, graduated in difficulty: 2 mid-level, 2 senior, 1 staff/principal.
For each:
- The question as an interviewer would phrase it
- A model answer (3–6 sentences) at senior engineer level
- The follow-up the interviewer will ask next
- The trap/pitfall candidates fall into

### 9. Trade-offs to articulate
For every major design decision in this problem, provide the explicit trade-off statement in the format:
"I chose [X] over [Y] because at this scale, [constraint], which means [Y's cost] outweighs [Y's benefit]. The trade-off I'm accepting is [Z]."
Minimum 5 trade-off statements.

### 10. Failure modes and resilience patterns
List the top 5 ways this system can fail in production. For each:
- How it manifests (symptoms)
- Root cause
- Detection method (what alert fires)
- Mitigation / recovery

### 11. How this connects forward
Which concepts introduced today will be directly required in Days {N+1} onward. Be specific — name the day and the exact dependency.

### 12. Diagrams to draw (descriptions only)
List 4–5 diagrams the learner should draw from memory on paper. Describe exactly what each diagram must show — components, data flow, failure paths. These should be drawable in under 5 minutes each.

## Format requirements
- Output raw markdown only — no preamble, no explanation outside the document
- Use H2 for section headers, H3 for sub-sections
- Use tables where comparisons are made
- Use code blocks for any algorithms, pseudocode, or config snippets
- Be data-dense. A senior engineer reading this should learn something new or have a concept sharpened — not just confirmed what they already know
- Target length: 1200–2000 words
