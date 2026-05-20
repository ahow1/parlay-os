web: python api.py
worker: python brain.py --bot
health: python health_check.py --loop
# HEROKU SCHEDULER NOTE: all brain.py cron times are in ET (America/New_York).
# The scheduler internally converts UTC→ET via pytz, so firing at any UTC time
# is safe — but set TZ=America/New_York in Config Vars so logs are readable.
