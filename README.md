# Show My Solutions
Update submissions from OJs onto Trello

## Components
### OJ Scraper
+ Keys: [REST](http://rest.elkstein.org/), HTML, Beautiful Soup, Python data structure, Install Python packages
+ Download the most recent and accepted submissions from an online judge website
  + For LeetCode, the website to scrap is https://leetcode.com/submissions/1 (you need to login first)
+ Parse the HTML or any downloaded data into native Python data structures using Beautiful Soup
+ Optimize: ask the database manager for the latest scraped submission and make requests only if necessary
+ Requests is a wonderful library
+ There are a bunch of leetcode scraper right on Github

### Database manager
+ Keys: SQL, sqlite, SQLAlchemy, Python data structure, Install Python packages
+ Design and create tables
+ Insert scraped entries to the database without duplication
+ Read and return entries since a certain date
+ Use the built-in sqlite for convenience
+ Use SQLAlchemy or plain SQL queries to manipulate the database

### Trello uploader
+ Keys: Learn to use the API of a website, REST, Python data structure, Install Python packages
+ Apply for an API key and learn to use it [here](https://developers.trello.com/)
+ Create a robot account on Trello and add to our team so that we don't have to apply for a key for each of our accounts
+ Ask the database manager for the latest uploaded submission to prevent duplication
+ There is a python wrapper of the Trello API on Github if you don't want to interact with REST API directly

### Main
+ Claimer: Simon
+ Keys: Asynchronicity / Multi-threading
+ Ask the scraper for data, pass it to the database manager, update states in the database, and pass the data to the uploader
+ Optimize: use asynchronous or Multi-threading framework
