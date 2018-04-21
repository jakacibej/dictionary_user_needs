# EXTRACTOR OF POSTS AND COMMENTS FROM FACEBOOK GROUPS FOR DICTIONARY USER RESEARCH
# Author(s): Jaka Čibej, based on the script by Max Woolf (https://github.com/minimaxir)
# Centre for Language Resources and Technologies (University of Ljubljana)
# https://www.cjvt.si/en/
# GitHub repository: https://github.com/jakacibej/dictionary_user_needs
# Max Woolf's version: https://github.com/minimaxir/facebook-page-post-scraper
# Version 1.0, last updated: 1 April 2018
# Creative Commons Attribution-ShareAlike 4.0 International (CC BY-SA 4.0)

# IMPORT LIBRARIES
import urllib.request
import json
import datetime
import csv
import time
import os
from collections import defaultdict as dd

# FACEBOOK API ID AND SECRET CODE
app_id = "<insert app ID>"
app_secret = "<insert app secret ID>"  # DO NOT SHARE SECRET CODE WITH ANYONE!


# GROUP ID
# Observe the source code of the Facebook group page and
# look for the 'entity_id' attribute to find the ID number
group_id = "<insert group ID>"

# ACCESS TOKEN
group_type = "<insert either 'public' or 'closed'>"
if group_type == 'public':
    # For PUBLIC GROUPS, the access token is the combination of the app ID and the app secret code
    access_token = app_id + "|" + app_secret
elif group_type == 'closed':
    # For CLOSED GROUPS, the access token must be acquired from the administrator's Facebook API app
    # The token must include the permission for USER_MANAGED_GROUPS
    # The token is not permanent and expires!
    # It is advisable to write down the expiry date here to keep track (e.g. June 11, 2018).
    access_token = '<insert access token>'
else:
    print("Error: Please specify group type ('public' or 'closed').")

# ANONYMIZATION OF USERNAMES
# Set to TRUE: All usernames will be converted to User1, User2, etc.
# Set to FALSE: Usernames are kept in their original form. Respect user data privacy!
anonymization = True
dictionary_of_usernames = dd()
user_counter = 0

# HELP FUNCTIONS
def request_until_succeed(url):
    req = urllib.request.Request(url)
    success = False
    while success is False:
        try: 
            response = urllib.request.urlopen(req)
            if response.getcode() == 200:
                success = True
        except Exception as e:
            print(e)
            time.sleep(5)

            print("Error for URL %s: %s" % (url, datetime.datetime.now()))
            print("Retrying.")

            if '400' in str(e):
                return None

    return response.read().decode('utf-8')

def unicode_normalize(text):

    # Correctly converts text to UTF-8
    return text.translate({ 0x2018:0x27, 0x2019:0x27, 0x201C:0x22, 0x201D:0x22,
                            0xa0:0x20 })#.encode('utf-8')

# CORE FUNCTIONS
def getFacebookPageFeedData(group_id, access_token, num_statuses):

    # Construct the URL string for the API call; see
    # http://stackoverflow.com/a/37239851 for Reactions parameters
    base = "https://graph.facebook.com/v2.9"
    node = "/%s/feed" % group_id
    fields = "&fields=message,link,created_time,object_id,source,picture,type,name,id," + \
             "comments.limit(0).summary(true),shares,reactions." + \
             "limit(0).summary(true),from"
    parameters = "/?limit=%s&access_token=%s" % (num_statuses, access_token)
    url = base + node + parameters + fields

    # Retrieve data in JSON format
    data = json.loads(request_until_succeed(url))

    return data

def getReactionsForStatus(status_id, access_token):

    # See http://stackoverflow.com/a/37239851 for Reactions parameters
        # Reactions are only accessible at a single-post endpoint

    base = "https://graph.facebook.com/v2.6"
    node = "/%s" % status_id
    reactions = "/?fields=" \
            "reactions.type(LIKE).limit(0).summary(total_count).as(like)" \
            ",reactions.type(LOVE).limit(0).summary(total_count).as(love)" \
            ",reactions.type(WOW).limit(0).summary(total_count).as(wow)" \
            ",reactions.type(HAHA).limit(0).summary(total_count).as(haha)" \
            ",reactions.type(SAD).limit(0).summary(total_count).as(sad)" \
            ",reactions.type(ANGRY).limit(0).summary(total_count).as(angry)"
    parameters = "&access_token=%s" % access_token
    url = base + node + reactions + parameters

    # Retrieve data
    data = json.loads(request_until_succeed(url))


    return data

def processFacebookPageFeedStatus(status, access_token, anonymization_type, dictionary_of_users, user_counter):

    # The Facebook status was retrieved in JSON format and is now a Python dictionary.
    # Top-level items are reached by calling the key.

    # Additionally, some items may not always exist.
    # In this case, the script first checks if they are present in the dictionary.

    # POST ID
    status_id = status['id']

    # THE TEXT OF THE POST
    status_message = '' if 'message' not in status.keys() else \
            unicode_normalize(status['message']) and status['message'].replace("\n", " ")

    # NAME OF THE LINK IF THE POST CONTAINS A LINK
    link_name = '' if 'name' not in status.keys() else \
            unicode_normalize(status['name'])

    # POST TYPE (e.g. STATUS, VIDEO, PHOTO)
    status_type = status['type']

    # LINK URL (IF INCLUDED IN THE POST)
    status_link = '' if 'link' not in status.keys() else \
            unicode_normalize(status['link'])

    # USERNAME
    try:
        status_author = unicode_normalize(status['from']['name'])
        if anonymization_type:
            if status_author not in dictionary_of_users:
                user_counter += 1
                dictionary_of_users[status_author] = "User_%s" % user_counter
                status_author = "User_%s" % user_counter
            else:
                status_author = dictionary_of_users[status_author]
    except:
        # If the user has deleted their account, the username is not available
        status_author = "Anonymous"

    # PHOTO
    # If a photo is included in the post, it is downloaded to a separate folder.
    if status_type == "photo":
        object_data = json.loads(
            request_until_succeed(
                'https://graph.facebook.com/v2.6/%s/attachments/?fields=media&access_token=%s' % (status_id,
                                                                                                  access_token)))
        if not object_data['data'] == []:
            object_src = object_data['data'][0]['media']['image']['src']
            photo_filename = "%s.jpeg" % (status_id)
            photo_folder = os.getcwd() + "/status_photos_%s" % group_id
            if not os.path.exists(photo_folder):
                os.makedirs(photo_folder)
            photo_filepath = photo_folder + "/" + photo_filename
            if not os.path.isfile(photo_filepath):
                print("Collecting image...")
                try:
                    urllib.request.urlretrieve(object_src, photo_filepath)
                except:
                    pass
            else:
                print("Image already in store.")
                pass

    # TIME OF PUBLICATION
    status_published = datetime.datetime.strptime(status['created_time'],'%Y-%m-%dT%H:%M:%S+0000')

    # SET TIME ZONE
    status_published = status_published + datetime.timedelta(hours=1) #GMT+1
    #status_published = status_published + datetime.timedelta(hours=-5) # EST

    # Convert to %Y-%m-%d %H:%M:%S (best format for spreadsheet programs)
    status_published = status_published.strftime('%Y-%m-%d %H:%M:%S')

    # NUMBER OF REACTIONS
    num_reactions = 0 if 'reactions' not in status else \
            status['reactions']['summary']['total_count']

    # NUMBER OF COMMENTS
    num_comments = 0 if 'comments' not in status else \
            status['comments']['summary']['total_count']

    # NUMBER OF SHARES
    num_shares = 0 if 'shares' not in status else \
            status['shares']['count']

    # NUMBER OF DIFFERENT TYPES OF REACTIONS
    # Reactions were implemented 24 February 2016.
    # Posts posted before that date only have the number of likes.
    # http://newsroom.fb.com/news/2016/02/reactions-now-available-globally/

    reactions = getReactionsForStatus(status_id, access_token) \
            if status_published > '2016-02-24 00:00:00' else {}

    num_likes = 0 if 'like' not in reactions else \
            reactions['like']['summary']['total_count']

    # Special case: Set number of Likes to Number of reactions for pre-reaction
    # statuses

    num_likes = num_reactions if status_published < '2016-02-24 00:00:00' else num_likes

    def get_num_total_reactions(reaction_type, reactions):
        if reaction_type not in reactions:
            return 0
        else:
            return reactions[reaction_type]['summary']['total_count']

    num_loves = get_num_total_reactions('love', reactions)
    num_wows = get_num_total_reactions('wow', reactions)
    num_hahas = get_num_total_reactions('haha', reactions)
    num_sads = get_num_total_reactions('sad', reactions)
    num_angrys = get_num_total_reactions('angry', reactions)

    # Return a tuple of all processed data

    return (status_id, status_message, status_author, link_name, status_type, 
            status_link, status_published, num_reactions, num_comments, 
            num_shares,  num_likes, num_loves, num_wows, num_hahas, num_sads, 
            num_angrys)

def scrapeFacebookPageFeedStatus(group_id, access_token):
    with open('%s_FB_statuses.csv' % group_id, 'w', newline='', encoding='utf-8') as file:
        w = csv.writer(file)
        w.writerow(["status_id", "status_message", "status_author", 
            "link_name", "status_type", "status_link",
            "status_published", "num_reactions", "num_comments", 
            "num_shares", "num_likes", "num_loves", "num_wows", 
            "num_hahas", "num_sads", "num_angrys"])

        has_next_page = True
        num_processed = 0   # Keep a count on how many posts were processed
        scrape_starttime = datetime.datetime.now()

        print("Scraping %s Facebook Page: %s\n" % (group_id, scrape_starttime))

        statuses = getFacebookPageFeedData(group_id, access_token, 100)

        while has_next_page:
            for status in statuses['data']:

                # Ensure it is a status with the expected metadata
                if 'reactions' in status:            
                    w.writerow(processFacebookPageFeedStatus(status, access_token, anonymization_type=anonymization, dictionary_of_users=dictionary_of_usernames, user_counter=user_counter))

                # output progress occasionally to make sure code is not
                # stalling
                num_processed += 1
                if num_processed % 100 == 0:
                    print("%s statuses processed: %s" % (num_processed, datetime.datetime.now()))

            # If there is no next page, the process is done.
            if 'paging' in statuses.keys():
                statuses = json.loads(request_until_succeed(statuses['paging']['next']))
            else:
                has_next_page = False

        print("\nExtraction complete!\n%s statuses processed in %s" % \
                (num_processed, datetime.datetime.now() - scrape_starttime))

if __name__ == '__main__':
    scrapeFacebookPageFeedStatus(group_id, access_token)