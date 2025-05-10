import os
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse
import logging
from tqdm import tqdm
import argparse
from concurrent.futures import ThreadPoolExecutor
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pdf_scraper.log"),
        logging.StreamHandler()
    ]
)

class PDFScraper:
    def __init__(self, base_url, output_folder="exam_papers", verify_ssl=False, max_workers=5):
        """
        Initialize the PDF scraper with the target URL and output folder.
        
        Args:
            base_url (str): The base URL to scrape PDFs from
            output_folder (str): Folder to save downloaded PDFs
            verify_ssl (bool): Whether to verify SSL certificates
            max_workers (int): Maximum number of concurrent downloads
        """
        self.base_url = base_url
        self.output_folder = output_folder
        self.verify_ssl = verify_ssl
        self.max_workers = max_workers
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        })
        
        if not verify_ssl:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
            logging.info(f"Created output folder: {output_folder}")
    
    def get_page_content(self, url):
        """
        Fetch the content of the specified URL.
        
        Args:
            url (str): URL to fetch
            
        Returns:
            str: HTML content or None if request failed
        """
        try:
            response = self.session.get(url, timeout=10, verify=self.verify_ssl)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch {url}: {e}")
            return None
    
    def extract_pdf_links(self, html_content):
        """
        Extract all PDF links from the HTML content.
        
        Args:
            html_content (str): HTML content to parse
            
        Returns:
            list: List of PDF URLs
        """
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        pdf_links = []
        
        # Look for links with href ending in .pdf
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.lower().endswith('.pdf'):
                # Convert relative URLs to absolute URLs
                absolute_url = urljoin(self.base_url, href)
                pdf_links.append(absolute_url)
                
        logging.info(f"Found {len(pdf_links)} PDF links")
        return pdf_links
    
    def download_pdf(self, url, year=None, filename=None):
        """
        Download a PDF file from the given URL.
        
        Args:
            url (str): URL of the PDF to download
            year (str, optional): Year to organize files into subfolders
            filename (str, optional): Custom filename to save as, 
                                     if None, extract from URL
        
        Returns:
            bool: True if download was successful, False otherwise
        """
        if not filename:
            # Extract filename from URL
            filename = os.path.basename(url)
            # Clean up the filename
            filename = re.sub(r'[^\w\.-]', '_', filename)
            if not filename.lower().endswith('.pdf'):
                filename += '.pdf'
        
        # Create year subfolder if specified
        if year:
            year_folder = os.path.join(self.output_folder, year)
            if not os.path.exists(year_folder):
                os.makedirs(year_folder)
                logging.info(f"Created year folder: {year_folder}")
            filepath = os.path.join(year_folder, filename)
        else:
            filepath = os.path.join(self.output_folder, filename)
        
        # Skip if file already exists
        if os.path.exists(filepath):
            logging.info(f"File already exists, skipping: {filename}")
            return True
        
        try:
            response = self.session.get(url, stream=True, timeout=30, verify=self.verify_ssl)
            response.raise_for_status()
            
            # Get file size for progress bar
            file_size = int(response.headers.get('content-length', 0))
            
            # Download with progress bar
            with open(filepath, 'wb') as f, tqdm(
                desc=filename,
                total=file_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
            ) as progress_bar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        progress_bar.update(len(chunk))
            
            logging.info(f"Successfully downloaded: {filename}")
            return True
        
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to download {url}: {e}")
            # Clean up partial download if it exists
            if os.path.exists(filepath):
                os.remove(filepath)
            return False
    
    def download_pdfs_for_year(self, school_code, year):
        """
        Download all PDFs for a specific year and school code.
        
        Args:
            school_code (str): School code (e.g., 'sci' for Science)
            year (str): Year (e.g., '2024')
            
        Returns:
            tuple: (total_pdfs, successful_downloads)
        """
        year_url = f"{self.base_url}/{year}-{school_code}-exam-papers/"
        logging.info(f"Processing year {year} for {school_code}: {year_url}")
        
        # Get the page content
        html_content = self.get_page_content(year_url)
        if not html_content:
            logging.error(f"Failed to get page content for {year}. Skipping.")
            return 0, 0
        
        # Extract PDF links
        pdf_links = self.extract_pdf_links(html_content)
        if not pdf_links:
            logging.warning(f"No PDF links found for {year}.")
            return 0, 0
        
        # Download each PDF
        successful_downloads = 0
        for url in pdf_links:
            if self.download_pdf(url, year=year):
                successful_downloads += 1
        
        logging.info(f"Year {year} completed. Downloaded {successful_downloads} of {len(pdf_links)} PDFs.")
        return len(pdf_links), successful_downloads
    
    def download_all_years(self, school_code, start_year, end_year, use_threads=True):
        """
        Download PDFs for multiple years.
        
        Args:
            school_code (str): School code (e.g., 'sci' for Science)
            start_year (int): Starting year
            end_year (int): Ending year
            use_threads (bool): Whether to use multithreading for faster downloads
            
        Returns:
            dict: Summary of downloads by year
        """
        summary = {}
        years = list(range(start_year, end_year + 1))
        years.sort(reverse=True)  # Process newest years first
        
        logging.info(f"Starting PDF scraping for {school_code} from {start_year} to {end_year}")
        
        if use_threads:
            # Use multithreading to process years concurrently
            with ThreadPoolExecutor(max_workers=min(len(years), self.max_workers)) as executor:
                future_to_year = {
                    executor.submit(self.download_pdfs_for_year, school_code, str(year)): year 
                    for year in years
                }
                
                for future in future_to_year:
                    year = future_to_year[future]
                    try:
                        total, successful = future.result()
                        summary[year] = {'total': total, 'downloaded': successful}
                    except Exception as exc:
                        logging.error(f"Year {year} generated an exception: {exc}")
                        summary[year] = {'total': 0, 'downloaded': 0, 'error': str(exc)}
        else:
            # Process years sequentially
            for year in years:
                total, successful = self.download_pdfs_for_year(school_code, str(year))
                summary[year] = {'total': total, 'downloaded': successful}
                time.sleep(1)  # Small delay to avoid overwhelming the server
        
        return summary
    
    def extract_base_url(self):
        """
        Extract the base domain from the URL.
        
        Returns:
            str: The base domain URL
        """
        parsed_url = urlparse(self.base_url)
        return f"{parsed_url.scheme}://{parsed_url.netloc}"


def main():
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description='Download exam papers from multiple years.')
    parser.add_argument('--school', type=str, default='sci', help='School code (e.g., sci for Science)')
    parser.add_argument('--start-year', type=int, default=2015, help='Starting year')
    parser.add_argument('--end-year', type=int, default=2024, help='Ending year')
    parser.add_argument('--output', type=str, default='exam_papers', help='Output folder name')
    parser.add_argument('--no-threads', action='store_true', help='Disable multithreading')
    parser.add_argument('--verify-ssl', action='store_true', help='Enable SSL verification')
    parser.add_argument('--max-workers', type=int, default=3, help='Maximum worker threads')
    parser.add_argument('--base-url', type=str, default='https://exampapers.must.ac.ke', 
                       help='Base URL of the exam papers repository')
    
    args = parser.parse_args()
    
    # Create and run the scraper
    scraper = PDFScraper(
        base_url=args.base_url,
        output_folder=args.output,
        verify_ssl=args.verify_ssl,
        max_workers=args.max_workers
    )
    
    # Download PDFs for all years
    summary = scraper.download_all_years(
        school_code=args.school,
        start_year=args.start_year,
        end_year=args.end_year,
        use_threads=not args.no_threads
    )
    
    # Print summary
    print("\nDownload Summary:")
    print(f"School: {args.school}")
    print(f"Years: {args.start_year} to {args.end_year}")
    
    # Calculate totals
    total_found = sum(year_data['total'] for year_data in summary.values())
    total_downloaded = sum(year_data['downloaded'] for year_data in summary.values())
    
    print("\nYear-by-year breakdown:")
    for year in sorted(summary.keys(), reverse=True):
        data = summary[year]
        print(f"  {year}: Found {data['total']} PDFs, Downloaded {data['downloaded']}")
    
    # Print overall totals
    print(f"\nOverall: Found {total_found} PDFs, Downloaded {total_downloaded}")
    print(f"Files saved to: {os.path.abspath(args.output)}")


if __name__ == "__main__":
    main()