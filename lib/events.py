import gevent
from gevent import monkey; monkey.patch_all()

import datetime


def expire_stale_prefs(mongo_db):
    """
    Every day, clear out preferences objects that haven't been touched in > 30 days, in order to reduce abuse risk/space consumed
    """
    min_last_updated = time.mktime((datetime.datetime.utcnow() - datetime.timedelta(days=30)).timetuple())
    
    num_stale_prefs = mongo_db.preferences.find({'last_touched': {'$lte': min_last_updated}}).count()
    mongo_db.preferences.remove({'last_touched': {'$lte': min_last_updated}})
    logger.warn("REMOVED %i stale preferences objects" % num_stale_prefs)
    
    #call again in 1 day
    gevent.spawn_later(86400, expireStalePrefs, mongo_db)
