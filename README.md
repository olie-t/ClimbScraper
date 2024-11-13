Climb Stats Web Scraper Project
=====
### Scope changes

Since orginal inception the scope of this project has massively changed. Originally I wanted to use AWS as a learning tool as I was aiming for a more data focused career and wanted exposure to cloud tech. However I have since changed roles, and am now working as a DBA using azure cloud and SQL on prem. As such I have decided to instead use SQLite as my database, and move on to create some visualisations locally. 

I would still like to eventually have this as a website that is able to respond to user queries to provide infomation on climbing in the UK and Europe. However as my goals have changed in general, it seems more beneficial to get a "MVP" version up and running. If for no other reason then to keep me motivated to work on this project. 


_orginal readme maintained below_
#### Why this project?

This project was born from conversations from freinds of mine on a climbing trip. 

During a trip up to the peak district, in the pub after we were discussing the range of grades that are given to routes.

Specifically the variance in the reported grade from people climbing the route, and the "given" grade of the route.

This got me to thinking that it might be fun to try and build a database by scraping both the set grade, and the reported grades from forums, and then presenting them somehow.

As I am learning python, SQL, and trying to move my career into data engineering, this seemed the perfect idea to get to building a complete data pipeline!


### First steps

Whilst I use SQL a lot at work, my python is comparatively weak. As such it has taken me quite a long time just to get the basic scraper working.

The first version is availble in the repo in 2 files. One to check for webpage IDs to ensure they are valid crags. Then a second to scrape the needed information, put it into
a dataframe for preperation for inserting into a database.

This worked well, however I quickly got my home IP blocked by agressivly scraping. Lessons learn, be kind to the target webpage, and maybe use cycling proxies.

### Second steps

Next up I am setting myself up using an EC2 instance on AWS to run my scraper. Reasons being 2 fold 
	- Leaving my laptop running the scraper forever isnt ideal
	- Good practice for learning AWS tooling
	- Eventually I want to put the data into some form of RDS DB so I can build a basic webpage to present my findings

In addition, Ive dug out an old laptop and installed ubuntu linux on it, to get out of the windows hellscape. Which I am now writing this on!

The scope creep for this project is getting real.

Once I have managed to get my scripts onto the EC instance and working correctly, I will start work on the next version of the scraping scripts. I realised my first iteration
missed some very cruicial infomation. So will need a rewrite.

