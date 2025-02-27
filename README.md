# COBOL Streamhouse

Welcome to **COBOL Streamhouse**, a groundbreaking project that bridges the gap between legacy COBOL systems and the modern data stack. For decades, efforts to rewrite COBOL into newer languages have stalled, yet COBOL remains a backbone of critical industries. This project embraces COBOL's enduring presence by integrating it with cutting-edge technologies like **Apache Kafka** and **Apache Iceberg**, enabling streaming data solutions and modern data lakes directly from COBOL applications.

## Why COBOL Streamhouse?

COBOL powers an estimated **70% of global business transactions**, running on over **220 billion lines of code** across industries like banking, insurance, and government systems. Recent news highlights its resilience: in 2024 alone, financial institutions processed trillions of dollars through COBOL-based mainframes, with systems like those at major banks and the IRS proving its unmatched reliability. Despite attempts to modernize by rewriting it—spanning efforts from the 1980s to today—COBOL persists because it *just works*.

Instead of replacing it, COBOL Streamhouse brings the latest data technologies to COBOL:
- **Apache Kafka**: Real-time data streaming for COBOL applications.
- **Apache Iceberg**: A modern table format for building scalable data lakes.

This isn’t about abandoning legacy code—it’s about empowering it. More projects leveraging this foundation are coming soon, unlocking new possibilities for COBOL in the era of big data and AI.

## Features

- **Streaming Data with Kafka**: Integrate COBOL programs with Apache Kafka to process and stream data in real time.
- **Data Lakes with Iceberg**: Use Apache Iceberg to manage large-scale datasets, bringing ACID transactions and schema evolution to COBOL-driven data lakes.
- **Legacy Meets Modern**: No rewrites needed—extend existing COBOL systems with minimal disruption.
- **Scalable and Future-Ready**: Position COBOL applications to power next-gen analytics and AI workflows.

## COBOL Usage Today

Here’s why COBOL remains relevant, based on recent insights:
- **Volume**: Over **80% of in-person financial transactions** worldwide still run on COBOL.
- **Systems**: Major institutions like the U.S. Social Security Administration and global banks rely on COBOL mainframes, handling **$3 trillion in daily commerce**.
- **News Spotlight**: In 2024, COBOL made headlines as legacy systems successfully managed peak loads during economic shifts, while modernization debates raged on.

Attempts to phase it out have faltered—rewriting billions of lines of code is costly and risky. COBOL Streamhouse flips the script: why rewrite when you can *enhance*?

## Getting Started

### Prerequisites
- COBOL compiler (e.g., GnuCOBOL or IBM Enterprise COBOL)
- A willingness to bring the future to the past

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/[your-username]/cobol-streamhouse.git
2. Set up Kafka and Iceberg dependencies (details in ```docs/setup.md```).
3. Compile the COBOL examples:
```bash
cobc -x examples/streamhouse.cbl
```
### Example
Run a simple COBOL program that streams data to Kafka and writes to an Iceberg table:
```cobol
       IDENTIFICATION DIVISION.
       PROGRAM-ID. STREAMHOUSE-DEMO.
       PROCEDURE DIVISION.
           DISPLAY "Streaming to Kafka from COBOL...".
           CALL "KAFKA-PUBLISH" USING "topic-name" "Hello, Iceberg!".
           DISPLAY "Writing to Iceberg table...".
           CALL "ICEBERG-WRITE" USING "table-name" "data-record".
           STOP RUN.
```
### Roadmap
- Kafka producer/consumer implementations in COBOL
- Iceberg table management utilities
- Integration with cloud providers (AWS, Azure, GCP)
- Sample applications for banking and insurance use cases
- Community-driven COBOL data stack extensions
### Contributing
We’re building the future of COBOL together! Check out ```CONTRIBUTING.md``` for guidelines. Whether you’re a COBOL veteran or a data engineer, your expertise is welcome.
### License
This project is licensed under the MIT License—see ```LICENSE``` for details.
### Contact
Have questions or ideas? Open an issue or reach out to the maintainers at [cobol@deltastream.io].
Let’s make COBOL the powerhouse of the modern data world!

