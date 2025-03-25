import requests
import json
import time
import csv
import logging
import argparse
from datetime import datetime, timedelta
import multiprocessing
from multiprocessing import Pool, Manager
from tqdm import tqdm
from urllib.parse import quote

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WikipediaStatsCollector:
	def __init__(self, user_agent, language="en", timeout=10):
		self.user_agent = user_agent
		self.language = language
		self.timeout = timeout
		self.session = requests.Session()
		self.session.headers.update({'User-Agent': user_agent})
		self.rest_api_base = f"https://{language}.wikipedia.org/api/rest_v1"
		self.action_api_base = f"https://{language}.wikipedia.org/w/api.php"
		self.metrics_api_base = "https://wikimedia.org/api/rest_v1"

	def _get_page_id(self, title):
		"""Helper to get page id for a given title"""
		params = {
			'action': 'query',
			'format': 'json',
			'titles': title
		}
		response = self.session.get(self.action_api_base, params=params, timeout=self.timeout)
		response.raise_for_status()
		data = response.json()
		page_id = list(data['query']['pages'].keys())[0]
		if int(page_id) < 0:
			return None
		return page_id

	def get_page_info(self, title):
		"""Get basic page information"""
		params = {
			'action': 'query',
			'format': 'json',
			'formatversion': 2,
			'titles': title,
			'prop': 'info|categories|revisions',
			'inprop': 'url|length|protection',
			'rvprop': 'ids|timestamp|size',
			'rvlimit': 1,
			'cllimit': 500
		}
		try:
			response = self.session.get(self.action_api_base, params=params, timeout=self.timeout)
			response.raise_for_status()
			data = response.json()
			if 'query' in data and 'pages' in data['query'] and data['query']['pages']:
				page_data = data['query']['pages'][0]
				if 'missing' in page_data:
					logger.warning(f"Page '{title}' does not exist")
					return None

				last_edit_timestamp = None
				if 'revisions' in page_data and page_data['revisions']:
					last_edit_timestamp = page_data['revisions'][0].get('timestamp')
					rev_size = page_data['revisions'][0].get('size', 0)
				else:
					rev_size = 0

				page_size = page_data.get('length', 0) or rev_size or page_data.get('size', 0)
				page_id = str(page_data.get('pageid', '0'))
				categories = [cat.get('title', '').replace('Category:', '')
							  for cat in page_data.get('categories', [])]

				return {
					'page_id': page_id,
					'title': page_data.get('title'),
					'size': page_size,
					'categories': categories,
					'last_revision_id': page_data.get('lastrevid'),
					'last_edit_timestamp': last_edit_timestamp,
					'protection': page_data.get('protection', [])
				}
			else:
				# Fallback to older format if necessary
				params.update({'formatversion': 1})
				response = self.session.get(self.action_api_base, params=params, timeout=self.timeout)
				response.raise_for_status()
				data = response.json()
				page_id = list(data['query']['pages'].keys())[0]
				page_data = data['query']['pages'][page_id]
				if 'missing' in page_data:
					logger.warning(f"Page '{title}' does not exist")
					return None
				last_edit_timestamp = None
				rev_size = 0
				if 'revisions' in page_data and page_data['revisions']:
					last_edit_timestamp = page_data['revisions'][0].get('timestamp')
					rev_size = page_data['revisions'][0].get('size', 0)
				# Try to get the page length directly
				length_params = {
					'action': 'query',
					'format': 'json',
					'prop': 'info',
					'inprop': 'length',
					'titles': title
				}
				try:
					length_response = self.session.get(self.action_api_base, params=length_params, timeout=self.timeout)
					length_response.raise_for_status()
					length_data = length_response.json()
					page_length = length_data['query']['pages'][page_id].get('length', 0)
				except Exception:
					page_length = 0

				page_size = page_length if page_length > 0 else rev_size or page_data.get('size', 0) or page_data.get('length', 0)
				categories = [cat.get('title', '').replace('Category:', '')
							  for cat in page_data.get('categories', [])]

				return {
					'page_id': page_id,
					'title': page_data.get('title'),
					'size': page_size,
					'categories': categories,
					'last_revision_id': page_data.get('lastrevid'),
					'last_edit_timestamp': last_edit_timestamp,
					'protection': page_data.get('protection', [])
				}
		except requests.exceptions.RequestException as e:
			logger.error(f"Error fetching page info for {title}: {e}")
			return None

	def get_revision_count(self, title):
		"""Total number of revisions for a page"""
		try:
			page_id = self._get_page_id(title)
			if not page_id:
				return 0

			count = 0
			rvcontinue = None
			while True:
				params = {
					'action': 'query',
					'format': 'json',
					'prop': 'revisions',
					'titles': title,
					'rvlimit': 'max',
					'rvprop': 'ids'
				}
				if rvcontinue:
					params['rvcontinue'] = rvcontinue

				response = self.session.get(self.action_api_base, params=params, timeout=self.timeout)
				response.raise_for_status()
				data = response.json()
				page_data = data['query']['pages'][page_id]
				revisions = page_data.get('revisions', [])
				count += len(revisions)
				if 'continue' in data:
					rvcontinue = data['continue'].get('rvcontinue')
				else:
					break

			return count
		except requests.exceptions.RequestException as e:
			logger.error(f"Error fetching revision count for {title}: {e}")
			return 0

	def get_pageviews(self, title, days=30):
		"""Pageviews statistics for a page over a specified number of days"""
		end_date = datetime.now()
		start_date = end_date - timedelta(days=days)
		start_str = start_date.strftime('%Y%m%d')
		end_str = end_date.strftime('%Y%m%d')
		title_encoded = quote(title.replace(' ', '_'), safe='')
		url = f"{self.metrics_api_base}/metrics/pageviews/per-article/{self.language}.wikipedia/all-access/all-agents/{title_encoded}/daily/{start_str}/{end_str}"
		try:
			response = self.session.get(url, timeout=self.timeout)
			response.raise_for_status()
			data = response.json()
			views_data = data.get('items', [])
			total_views = sum(item['views'] for item in views_data)
			average_daily_views = total_views / len(views_data) if views_data else 0
			return {
				'total_views': total_views,
				'average_daily_views': round(average_daily_views, 2),
				'days_measured': len(views_data)
			}
		except requests.exceptions.RequestException as e:
			logger.error(f"Error fetching pageviews for {title}: {e}")
			return {'total_views': 0, 'average_daily_views': 0, 'days_measured': 0}

	def get_contributors(self, title):
		"""Total number of unique contributors to a page"""
		try:
			page_id = self._get_page_id(title)
			if not page_id:
				return {'total_contributors': 0}

			total_contributors = 0
			pccontinue = None
			while True:
				params = {
					'action': 'query',
					'format': 'json',
					'titles': title,
					'prop': 'contributors',
					'pclimit': 'max'
				}
				if pccontinue:
					params['pccontinue'] = pccontinue

				response = self.session.get(self.action_api_base, params=params, timeout=self.timeout)
				response.raise_for_status()
				data = response.json()
				page_data = data['query']['pages'][page_id]
				contributors = page_data.get('contributors', [])
				total_contributors += len(contributors)
				if 'continue' in data:
					pccontinue = data['continue'].get('pccontinue')
				else:
					break

			return {'total_contributors': total_contributors}
		except requests.exceptions.RequestException as e:
			logger.error(f"Error fetching contributors for {title}: {e}")
			return {'total_contributors': 0}

	def get_references_count(self, title):
		"""Get the number of references in the article using various methods"""
		try:
			parse_params = {
				'action': 'parse',
				'format': 'json',
				'page': title,
				'prop': 'text'
			}
			response = self.session.get(self.action_api_base, params=parse_params, timeout=self.timeout)
			response.raise_for_status()
			data = response.json()
			if 'parse' in data and 'text' in data['parse']:
				html_content = data['parse']['text'].get('*', '')
				ref_count = html_content.count('<li id="cite_note')
				if ref_count == 0:
					ref_count = html_content.count('<span class="reference-text')
				if ref_count == 0:
					ref_count = html_content.count('class="citation')
				if ref_count == 0:
					ref_count = html_content.count('</ref>')
				return {'references_count': ref_count}

			# Fallback: use sections or raw wikitext
			params = {
				'action': 'parse',
				'format': 'json',
				'page': title,
				'prop': 'sections'
			}
			response = self.session.get(self.action_api_base, params=params, timeout=self.timeout)
			response.raise_for_status()
			data = response.json()
			for section in data.get('parse', {}).get('sections', []):
				if section.get('line', '').lower() in ('references', 'notes', 'citations', 'sources', 'footnotes'):
					section_index = section.get('index')
					params = {
						'action': 'parse',
						'format': 'json',
						'page': title,
						'section': section_index
					}
					response = self.session.get(self.action_api_base, params=params, timeout=self.timeout)
					response.raise_for_status()
					data = response.json()
					html_content = data.get('parse', {}).get('text', {}).get('*', '')
					ref_count = html_content.count('<li id="cite_note')
					if ref_count == 0:
						ref_count = html_content.count('<span class="reference-text')
					if ref_count == 0:
						ref_count = html_content.count('<li')
					return {'references_count': ref_count}
			# Last resort: count <ref> tags in raw wikitext
			wikitext_params = {
				'action': 'parse',
				'format': 'json',
				'page': title,
				'prop': 'wikitext'
			}
			response = self.session.get(self.action_api_base, params=wikitext_params, timeout=self.timeout)
			response.raise_for_status()
			data = response.json()
			if 'parse' in data and 'wikitext' in data['parse']:
				wikitext = data['parse']['wikitext'].get('*', '')
				ref_count = wikitext.count('<ref')
				return {'references_count': ref_count}
			return {'references_count': 0}
		except Exception as e:
			logger.error(f"Error fetching references for {title}: {e}")
			try:
				wikitext_params = {
					'action': 'parse',
					'format': 'json',
					'page': title,
					'prop': 'wikitext'
				}
				response = self.session.get(self.action_api_base, params=wikitext_params, timeout=self.timeout)
				response.raise_for_status()
				data = response.json()
				if 'parse' in data and 'wikitext' in data['parse']:
					wikitext = data['parse']['wikitext'].get('*', '')
					ref_count = wikitext.count('<ref')
					return {'references_count': ref_count}
			except Exception:
				pass
			return {'references_count': 0}

	def get_full_stats(self, title):
		"""Compile complete statistics for a Wikipedia article"""
		try:
			page_info = self.get_page_info(title)
			if not page_info:
				return {'title': title, 'exists': False}

			pageviews = self.get_pageviews(title)
			contributors = self.get_contributors(title)
			total_edits = self.get_revision_count(title)
			references = self.get_references_count(title)

			last_edit_days = None
			if page_info.get('last_edit_timestamp'):
				try:
					last_edit_date = datetime.strptime(page_info['last_edit_timestamp'], '%Y-%m-%dT%H:%M:%SZ')
					last_edit_days = (datetime.now() - last_edit_date).days
				except Exception:
					last_edit_days = None

			is_protected = any(prot.get('type') in ('edit', 'move') for prot in page_info.get('protection', []))
			stats = {
				'title': title,
				'exists': True,
				'page_id': page_info['page_id'],
				'size_bytes': page_info['size'],
				'categories': page_info['categories'],
				'category_count': len(page_info['categories']),
				'total_views_30days': pageviews['total_views'],
				'average_daily_views': pageviews['average_daily_views'],
				'total_contributors': contributors['total_contributors'],
				'total_edits': total_edits,
				'references_count': references['references_count'],
				'last_revision_id': page_info['last_revision_id'],
				'last_edit_timestamp': page_info.get('last_edit_timestamp'),
				'days_since_last_edit': last_edit_days,
				'is_protected': is_protected
			}
			return stats
		except Exception as e:
			logger.error(f"Error collecting stats for {title}: {e}")
			return {'title': title, 'exists': False, 'error': str(e)}

def process_article(args):
	"""Process a single article and return stats"""
	title, user_agent, language = args
	collector = WikipediaStatsCollector(user_agent=user_agent, language=language)
	try:
		stats = collector.get_full_stats(title)
		if 'categories' in stats and isinstance(stats['categories'], list):
			stats['all_categories'] = json.dumps(stats['categories'])
			stats['categories'] = ','.join(stats['categories'])
		stats['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		for field in ['size_bytes', 'total_views_30days', 'average_daily_views', 'total_edits', 'references_count']:
			if field in stats and stats[field] == 0:
				logger.warning(f"Zero value for {field} in article {title}, which may indicate an API issue")
		return stats
	except Exception as e:
		logger.error(f"Error processing article {title}: {e}")
		return {'title': title, 'exists': False, 'error': str(e)}

def csv_writer_process(output_queue, output_file, fields, done_event):
	"""Handles CSV writing"""
	try:
		all_fields = set(fields)
		rows_to_write = []
		initial_batch_size = 5
		initial_rows = 0
		logger.info("Collecting initial rows to determine all fields")
		while initial_rows < initial_batch_size:
			if done_event.is_set() and output_queue.empty():
				break
			try:
				row = output_queue.get(timeout=0.1)
				rows_to_write.append(row)
				all_fields.update(row.keys())
				initial_rows += 1
			except Exception:
				if done_event.is_set():
					break
				time.sleep(0.1)
				continue

		key_fields = [f for f in fields if f in all_fields]
		other_fields = [f for f in all_fields if f not in key_fields]
		all_field_names = key_fields + sorted(other_fields)
		logger.info(f"Writing CSV with {len(all_field_names)} fields")
		with open(output_file, 'w', newline='', encoding='utf-8') as f:
			writer = csv.DictWriter(f, fieldnames=all_field_names, extrasaction='ignore')
			writer.writeheader()
			for row in rows_to_write:
				writer.writerow(row)
				f.flush()
			while not (done_event.is_set() and output_queue.empty()):
				try:
					row = output_queue.get(timeout=0.1)
					writer.writerow(row)
					f.flush()
				except Exception:
					continue
	except Exception as e:
		logger.error(f"CSV writer process error: {e}")

def read_titles_from_file(filename):
	"""Read article titles from the orgin file"""
	try:
		with open(filename, 'r', encoding='utf-8') as f:
			titles = [line.strip() for line in f if line.strip()]
		logger.info(f"Read {len(titles)} article titles from {filename}")
		return titles
	except IOError as e:
		logger.error(f"Error reading titles from file: {e}")
		return []

def main():
	parser = argparse.ArgumentParser(description='Collect Wikipedia article statistics')
	parser.add_argument('--input', '-i', required=True, help='File containing article titles, one per line')
	parser.add_argument('--output', '-o', default='wikipedia_stats.csv', help='Output CSV file')
	parser.add_argument('--language', '-l', default='en', help='Wikipedia language code (default: en)')
	parser.add_argument('--processes', '-p', type=int, default=multiprocessing.cpu_count(), 
						help=f'Number of parallel processes (default: {multiprocessing.cpu_count()})')
	parser.add_argument('--batch-size', '-b', type=int, default=10, help='Batch size for processing (default: 10)')
	args = parser.parse_args()

	user_agent = f"WikiStats/1.0 (https://github.com/your-repo/wikistats; your@email.com)"
	titles = read_titles_from_file(args.input)
	if not titles:
		logger.error("No article titles found. Exiting.")
		return

	manager = Manager()
	output_queue = manager.Queue()
	done_event = manager.Event()

	csv_fields = ['title', 'exists', 'page_id', 'size_bytes', 'total_views_30days', 
				  'average_daily_views', 'total_contributors', 'total_edits', 
				  'category_count', 'references_count', 'last_revision_id', 'last_edit_timestamp', 
				  'days_since_last_edit', 'is_protected', 'categories', 'error']

	csv_process = multiprocessing.Process(
		target=csv_writer_process,
		args=(output_queue, args.output, csv_fields, done_event)
	)
	csv_process.start()

	try:
		logger.info(f"Processing {len(titles)} articles using {args.processes} processes")
		with Pool(processes=args.processes) as pool:
			article_args = [(title, user_agent, args.language) for title in titles]
			successful = 0
			for i, stats in enumerate(tqdm(pool.imap_unordered(process_article, article_args),
											 total=len(titles),
											 desc="Processing articles")):
				output_queue.put(stats)
				if stats.get('exists', False):
					successful += 1
				if (i + 1) % 10 == 0 or i == len(titles) - 1:
					logger.info(f"Processed {i + 1}/{len(titles)} articles")
		done_event.set()
		csv_process.join(timeout=10)
		logger.info(f"Processing complete: {successful}/{len(titles)} articles processed successfully")
	except KeyboardInterrupt:
		logger.info("Process interrupted by user")
	except Exception as e:
		logger.error(f"Error during processing: {e}")
	finally:
		if csv_process.is_alive():
			csv_process.terminate()
			csv_process.join()
		logger.info(f"Results saved to {args.output}")

if __name__ == "__main__":
	multiprocessing.freeze_support()
	main()
