# CobBerg: Apache Iceberg in COBOL

       Identification Division.
       Program-Id. CobBerg-Readme.
       Author. xAI-Grok-Team.
       Date-Written. Feb-26-2025.

Welcome to **CobBerg**, a COBOL-based implementation of [Apache Iceberg](https://iceberg.apache.org/), the open table format for massive analytic datasets. This project doesn’t fight COBOL’s enduring grip on the world—it leans into it, bringing modern data lake capabilities to a language that’s too stubborn (and too critical) to fade away.

## Why CobBerg?
```COBOL
       Environment Division.
       Data Division.
       Working-Storage Section.
       01  COBOL-Fact         Pic X(50) Value "COBOL Runs the World".
       01  Rewrite-Cost      Pic 9(12)v99 Value 9999999999.99.
```
COBOL powers the backbone of finance, insurance, and government—trillions of transactions daily on systems too entrenched to replace. Rewriting them in something shiny like Python? That’s a billion-dollar fantasy no one’s cashing. So, we flipped the script: CobBerg adapts Apache Iceberg to COBOL, letting legacy systems join the data lake revolution without a single line of modernization they can’t handle.

## Features

- Iceberg Tables in COBOL: Schema evolution, partitioning, and snapshots, all in native Pic X and Pic 9.
- Legacy Integration: Fits into your batch jobs and JCL workflows like it was born there.
- File Format Support: Parquet, ORC, and Avro, mapped to COBOL Redefines for seamless access.
- No Rewrite Needed: Extends COBOL’s life by plugging it into the modern stack.
- Performance: Iceberg’s optimizations meets COBOL’s Perform Until efficiency.

## Getting Started

### Prerequisites

- A COBOL compiler ([GnuCOBOL](https://gnucobol.sourceforge.io/) or IBM Enterprise COBOL)
- COBOL runtime environment
- Apache Iceberg libraries (for metadata compatibility)
- A steady hand and respect for the old ways

### Installation

1. Clone the repository:
   git clone https://github.com/your-org/cobberg.git
2. Enter the directory:
   cd cobberg
3. Compile the source:
   cobc -x cobberg.cbl
4. Set your metadata and storage paths in the Config-Data section.
5. Run it:
   ./cobberg

### Example Usage
```COBOL
       Identification Division.
       Program-Id. CobBerg-Demo.
       Data Division.
       Working-Storage Section.
       01  Table-Name        Pic X(20) Value "Inventory".
       01  Snapshot-Id       Pic 9(18) Value 987654321098765432.
       Procedure Division.
           Call "CobBerg-Init" Using Table-Name.
           Call "CobBerg-Snapshot" Using Snapshot-Id.
           Display "CobBerg loaded snapshot: " Snapshot-Id
                   Upon Console.
           Stop Run.
```

## Project Structure

- ```cobberg.cbl```: The main COBOL program where the magic happens.
- ```lib/```: Copybooks for table metadata and file handling.
- ```docs/```: Extra notes and COBOL commenting wisdom.
- ```tests/```: Sample programs to ```ACCEPT``` and ```DISPLAY``` your way to validation.

### Why COBOL for Iceberg?

       Procedure Division.
       0100-Why-COBOL.
           Display "Modern stack wants new code".
           Display "COBOL says no".
           Display "CobBerg says yes to both".

The data world loves its cloud toys, but COBOL’s still grinding away in the enterprise engine room. Instead of forcing a rewrite, **CobBerg** rewrites the rules—bringing Iceberg’s power to COBOL with Move statements and all. It’s legacy meets lake, running smooth on that trusty mainframe.

### Contributing

Got a tighter Perform loop or a new Iceberg trick? We’d love your help:
1. Fork the repo.
2. Submit a pull request with your changes.
3. Comment your code—80 columns deserve some love.

### License

**CobBerg** is licensed under the Apache License 2.0 (LICENSE), matching Iceberg’s open-source roots.

Acknowledgments

- The Apache Iceberg crew for the table tech inspiration.
- COBOL coders everywhere, keeping the world spinning since ’59.
``` COBOL
       Stop Run.
``` 