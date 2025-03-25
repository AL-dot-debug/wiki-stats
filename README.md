# WikiStats

WikiStats is a Python script that collects comprehensive statistics for Wikipedia articles. It uses the MediaWiki API and Wikimedia metrics to gather various data points, such as edit counts, pageviews, contributors, and references. The script leverages multiprocessing to process multiple articles concurrently and outputs the results in a CSV file.

---

## Features

- **Page Information:** Retrieves basic page data such as page ID, title, size (in bytes), categories, and protection status.
- **Edit Count:** Uses API pagination to accurately count the total number of revisions (edits).
- **Pageviews:** Collects total and average daily pageviews over a specified period (default is 30 days).
- **Contributors:** Counts unique contributors with full pagination support.
- **References Count:** Estimates the number of references by parsing HTML content and raw wikitext.
- **Multiprocessing:** Processes multiple articles concurrently for faster results.
- **CSV Output:** Writes the collected statistics to a CSV file with customizable fields.

---

## Requirements

- **Python Version:** Python 3.6+
- **Python Packages:**
  - `requests`
  - `tqdm`

### Installation

Install the required packages using pip:

```bash
pip install requests tqdm
```

---

## Usage

1. **Prepare the Input File:**
   
   Create a text file (e.g., `articles`) containing a list of Wikipedia article titles, one per line.

2. **Run the Script:**

   Execute the script from the command line with the required arguments:

   ```bash
   python wiki-stats.py --input articles --output wikipedia_stats.csv --language en --processes 40
   ```

   **Arguments:**
   - `--input` or `-i`: Path to the file containing article titles (required).
   - `--output` or `-o`: Path to the output CSV file (default: `wikipedia_stats.csv`).
   - `--language` or `-l`: Wikipedia language code (default: `en`).
   - `--processes` or `-p`: Number of parallel processes (default: number of CPU cores).
   - `--batch-size` or `-b`: Batch size for processing (default: 10).

---

## Contributing

Contributions are welcome! If you encounter any issues or have suggestions for improvements, please open an issue or submit a pull request.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

## Disclaimer

This script is provided as-is without any warranty. Please use responsibly and be mindful of the Wikipedia API rate limits when running large batches of queries.