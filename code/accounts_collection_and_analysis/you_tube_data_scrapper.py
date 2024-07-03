# Info - Two external python client is used as part of the data fetching
# https://levelup.gitconnected.com/accessing-youtube-with-python-4e0c58d7044c
# https://pypi.org/project/youtube-search-python/
import time

from pytube import YouTube
from pytube import Playlist
from pytube import Channel
from youtubesearchpython import VideosSearch


class YouTubeData:
    def __init__(self, context):
        self.context = context

    def search_video(self):
        videos_search = VideosSearch(self.context, limit=50)
        _results = videos_search.result()
        if 'result' in _results:
            _data = _results['result']
            return _data
        return []

    def video_metadata(self):
        try:
            yt = YouTube(self.context)
            try:
                en_caption = yt.captions.get_by_language_code("en")
                captions = en_caption.generate_srt_captions()
            except:
                captions = None

            _data = {
                'title': yt.title,
                'thumbnail_url': yt.thumbnail_url,
                'views': yt.views,
                'video_id': yt.video_id,
                'channel_id': yt.channel_id,
                'channel_name': yt.author,
                'duration': yt.length,
                'description': yt.description,
                'publish_date': yt.publish_date,
                'tags': yt.keywords,
                'age_restricted': yt.age_restricted,
                'rating': yt.rating,
                'available_captions': str(yt.captions),
                'captions': captions,
                'link': self.context,
                'time': int(time.time() * 1000)
            }
            return _data
        except Exception as ex:
            _data = {'error':True, 'text': str(ex)}
            return _data

    def download_video(self):
        yt = YouTube(self.context)
        yt.streams.get_highest_resolution().download()

    def download_audio_only(self):
        yt = YouTube(self.context)
        yt.streams.get_audio_only().download()

    def get_video_urls_of_playlist(self):
        pl = Playlist(self.context)
        video_urls = pl.video_urls
        return video_urls

    def download_all_video_from_play_list(self):
        pl = Playlist(self.context)
        for video in pl.videos:
            video.streams.get_highest_resolution().download()

    def get_video_urls_of_channel(self):
        ch = Channel(self.context)
        return ch.video_urls

    def download_all_video_from_channel(self):
        ch = Channel(self.context)
        for video in ch.videos:
            video.streams.get_highest_resolution().download()