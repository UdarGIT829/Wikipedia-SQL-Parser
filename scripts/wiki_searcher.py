"""
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import argparse
import glob
import sqlite3

from utils import get_connection

def search_articles_by_title(title, limit=20, introductionOnly=True):
    results = []
    db_files = glob.glob('wikipedia_*.db')

    print(f"*"*20)
    print(f"Searching for Articles with string({title}):")

    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()
        _text = f"Checking {db_file}, querying..."
        print(_text)
        
        # Query to get articles by title
        query_articles = '''
        SELECT article_id, title FROM articles
        WHERE title LIKE ?
        LIMIT ?
        '''

        # Execute the article query
        c.execute(query_articles, (f'%{title}%', limit))
        articles = c.fetchall()

        _text = f"Found {len(articles)} articles..."
        print(_text)

        counter = 1
        for article_id, article_title in articles:
            _text = f"Gathering data for article #{counter}"
            print(_text)
            print(" "*len(_text))
            counter += 1
            article_data = {'id': article_id, 'title': article_title, 'sections': [], 'categories': [], 'redirects_to': None}


            # Query to get sections for the article
            if introductionOnly:
                query_sections = '''
                SELECT section_title, section_content, wikitables FROM article_sections
                WHERE article_id = ?
                ORDER BY id LIMIT 1
                '''
            else:
                query_sections = '''
                SELECT section_title, section_content, wikitables FROM article_sections
                WHERE article_id = ?
                '''
            
            # Execute the sections query
            c.execute(query_sections, (article_id,))
            sections = c.fetchall()
            article_data['sections'] = [{'title': section[0], 'content': section[1], 'wikitables': section[2]} for section in sections]

            # Query to get categories for the article
            query_categories = '''
            SELECT c.name FROM categories c
            INNER JOIN article_categories ac ON c.category_id = ac.category_id
            WHERE ac.article_id = ?
            '''

            # Execute the categories query
            c.execute(query_categories, (article_id,))
            categories = c.fetchall()
            article_data['categories'] = [category[0] for category in categories]

            # Check if the article is a redirect
            redirect_query = '''
            SELECT article_id FROM articles
            WHERE title = ?
            '''

            # Execute the redirect query
            c.execute(redirect_query, (article_title,))
            redirect_articles = c.fetchall()

            if redirect_articles:
                # Include immediate redirects (for disambiguations and such)
                if len(redirect_articles) > 1:
                    print(f"Design Flaw: Redirect for '{article_title}' resolves to multiple articles: {redirect_articles}")
                article_data['redirects_to'] = [redirect_article[0] for redirect_article in redirect_articles]
                results.append(article_data)
                break  # Break after the first article with a redirect
            else:
                # Else just add the article
                results.append(article_data)

        conn.close()

    return results


def search_articles_by_text(text, limit=100):
    results = []
    db_files = glob.glob('wikipedia_*.db')

    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()

        query = '''
        SELECT a.title, a.article_id, GROUP_CONCAT(c.name) as categories
        FROM articles a
        LEFT JOIN article_categories ac ON a.article_id = ac.article_id
        LEFT JOIN categories c ON ac.category_id = c.category_id
        WHERE a.article_id IN (
            SELECT article_id
            FROM article_sections
            WHERE section_content LIKE ?
        )
        GROUP BY a.article_id
        LIMIT ?
        '''
        
        c.execute(query, (f'%{text}%', limit))
        results.extend(c.fetchall())
        conn.close()

    return results

def search_articles_by_category(category, limit=100):
    results = []
    db_files = glob.glob('wikipedia_*.db')
    print()
    for db_file in db_files:
        _ = f"Checking DB: {db_file}"
        print(_,end="\r")
        conn = sqlite3.connect(db_file)
        c = conn.cursor()

        query = '''
        SELECT a.title, a.article_id
        FROM articles a
        JOIN article_categories ac ON a.article_id = ac.article_id
        JOIN categories c ON ac.category_id = c.category_id
        WHERE c.name LIKE ?
        LIMIT ?
        '''
        
        c.execute(query, (f'%{category}%', limit))
        results.extend(c.fetchall())
        conn.close()
        print(" "*len(_),end="\r")
    

    return results

def general_search(query, limit=100):
    title_results = search_articles_by_title(query, limit)
    text_results = search_articles_by_text(query, limit)
    category_results = search_articles_by_category(query, limit)
    
    return {
        "title_results": [{'title': iter_title_results[0], 'page_id': iter_title_results[1]} for iter_title_results in title_results],
        "text_results": [{'title': iter_text_results[0], 'page_id': iter_text_results[1]} for iter_text_results in text_results],
        "category_results": [{'title': iter_cat_results[0], 'page_id': iter_cat_results[1]} for iter_cat_results in category_results],
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search Wikipedia articles.")
    parser.add_argument("query", help="Search query for Wikipedia articles")
    args = parser.parse_args()

    query = args.query
    results = search_articles_by_title(query)
    import pprint
    pprint.pprint(results)