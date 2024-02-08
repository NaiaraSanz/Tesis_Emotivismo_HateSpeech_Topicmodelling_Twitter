import datetime
import json
import logging
import logging.handlers
import os
import sys
import traceback

import requests
import tweepy


class MyStreamListener(tweepy.StreamingClient):
	"""Streaming client that logs tweets to a file"""

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.logTwitter = logging.getLogger('twitterMsg')
		self.logError = logging.getLogger('error')
		self.total = 0
		self.contador = 0
		self.minuto = datetime.datetime.now().minute

	def on_tweet(self, status):
		"""Captures raw data received from Twitter"""

		print(status.data)

		try:
			self.logTwitter.info(json.dumps(status.data))
		except:
			self.logError.error(f'Error al volcar el tweet {json.dumps(status.data)}')

		self.contador += 1
		self.total += 1
		print("============== tweets totales: ", self.total, "================")
		date = datetime.datetime.now()
		minuto = date.minute
		if minuto != self.minuto:
			self.logError.info(f'{self.contador} tweets  -- Total {self.total} tweets')
			self.minuto = minuto
			self.contador = 0


def logs_setup(folder, basename):
	os.makedirs(folder, exist_ok=True)

	twitterMsg_log_handler = logging.handlers.TimedRotatingFileHandler(os.path.join(folder, basename), when='h', interval=1)
	twitterMsg_log = logging.getLogger('twitterMsg')
	twitterMsg_log.addHandler(twitterMsg_log_handler)
	twitterMsg_log.setLevel(logging.INFO)

	error_log = logging.getLogger('error')
	formatter = logging.Formatter('%(asctime)s : %(message)s')
	fileHandler = logging.FileHandler(os.path.join(folder, 'ERROR_MESSAGES'), mode='w')
	fileHandler.setFormatter(formatter)
	error_log.setLevel(logging.DEBUG)
	error_log.addHandler(fileHandler)


def search(keywords, stream):
	logError = logging.getLogger('error')

	# Add the filter rule to the stream
	keywords_form = ' OR '.join((f'"{key}"' if ' ' in key else key) for key in keywords)
	stream.add_rules(tweepy.StreamRule(
		# The rule format is defined in
		# https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query#list
		# Otros ejemplos: place_country:ES, @usuario, #hashtag, from:usuario, to:usuario, place:sitio,
		# is:reply, is:quote, has:images, etc.
		f'({keywords_form}) lang:es -is:retweet',
		tag='keywords',
	))

	while True:
		try:
			logError.info("Connecting to the stream, track=" + str(keywords))
			stream.filter(
				# Fields we want to recover from Twitter
				# https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
				tweet_fields=['id', 'text', 'author_id', 'created_at', 'geo', 'source']
			)
		except requests.exceptions.ConnectionError:
			logError.critical('ERROR: Max retries exceeded. Connection by Twitter refused. Exiting...')
			sys.exit()
		except KeyboardInterrupt:
			logError.info('Exiting normally')
			stream.disconnect()
			break
		except Exception as e:
			"""The most common exceptions are raised when the connection
			is lost or the tweets are arriving faster than they can
			be processed. We can ignore these exceptions, reconnect
			and continue collecting tweets."""
			logError.error(f'Exception: {type(e)} {e}.\n\n{traceback.format_exc()}\nNot critical. Continuing...')
			continue


if __name__ == "__main__":
	import argparse

	# Ejemplo de llamada al comando:
	#   {0} /home/kike/twitterMsgs/ Twitterclasico clásico clasico elclásico elclasico\n
	parser = argparse.ArgumentParser(description='Capturador de tweets')
	parser.add_argument('carpeta', help='Carpeta donde guardar los tweets capturados')
	parser.add_argument('nombre', help='Nombre para los ficheros que se van a generar')
	parser.add_argument('claves', help='Palabras clave', nargs='+')

	args = parser.parse_args()

	logs_setup(args.carpeta, args.nombre)

	# usa las credenciales cargadas desde credenciales_twitter
	from credenciales_twitter import BEARER_TOKEN
	stream = MyStreamListener(
		bearer_token=BEARER_TOKEN,
	)

	search(args.claves, stream)
