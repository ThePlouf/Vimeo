import urllib.request
import urllib.parse
import json
import pathlib
import base64
import os
import threading

videoList = [
    {
       'name': 'Class 4-28-20 for HF Community',
       'url': 'https://vimeo.com/411486465/35bfe05a6f'
    },
    {
       'name': 'a320',
       'url': 'https://vimeo.com/30630299'
    }
]


def is_public_video(url):
    return url.startswith('https://vimeo.com/')


def get_master_url_for_public_video(public_url):
    video_id = str(public_url).split('/')[3]
    config_url = 'https://player.vimeo.com/video/{}/config'.format(video_id)
    with urllib.request.urlopen(config_url) as config_response:
        config_data = json.loads(config_response.read())
        return config_data['request']['files']['dash']['cdns']['akfire_interconnect_quic']['url']


def for_each(function, collection):
    list(map(function, collection))


def do_sequential(jobs_to_run):
    for_each(lambda x: x(), jobs_to_run)


def do_parallel(jobs_to_run, n_threads=10):
    threads = [
        threading.Thread(
            target=lambda i=i: do_sequential([jobs_to_run[j] for j in range(i, len(jobs_to_run), n_threads)])
        )
        for i in range(0, n_threads)
    ]
    for_each(threading.Thread.start, threads)
    for_each(threading.Thread.join, threads)


def get_segment_path_name(filename, segment_index):
    return './segments/{}.segment.{}'.format(filename, segment_index+1)


def process_segment(type_name, segment_url, filename, segment_index, total_segment_count):
    pathlib.Path('segments').mkdir(parents=True, exist_ok=True)

    segment_path_name = get_segment_path_name(filename, segment_index)
    path = pathlib.Path(segment_path_name)
    if path.exists():
        print(segment_path_name + " already exists, skipping")
        return

    partial_file = pathlib.Path(segment_path_name + '.~partial')
    if partial_file.exists():
        os.remove(partial_file)

    with partial_file.open('w+b') as opened_file:
        with urllib.request.urlopen(segment_url) as segment_response:
            print('Downloading {} {} {}/{}'.format(segment_path_name, type_name, segment_index + 1, total_segment_count))
            opened_file.write(segment_response.read())

    partial_file.rename(path)


def process_file(type_name, base_url, init_data, segments, filename):
    pathlib.Path('parts').mkdir(parents=True, exist_ok=True)

    path_name = './parts/{}'.format(filename)
    path = pathlib.Path(path_name)
    if path.exists():
        print(path_name + " already exists, skipping")
        return path.absolute()

    segment_url_list = list(map(lambda segment: base_url + segment['url'], segments))
    segment_jobs_list = [lambda index=index, segment_url=segment_url: process_segment(type_name, segment_url, filename, index, len(segment_url_list)) for index, segment_url in enumerate(segment_url_list)]

    do_parallel(segment_jobs_list)

    print("Combining segments for {}".format(filename))

    init_buffer = base64.b64decode(init_data)

    partial_file = pathlib.Path(path_name + '.~partial')
    if partial_file.exists():
        os.remove(partial_file)

    with partial_file.open('w+b') as opened_file:
        opened_file.write(init_buffer)
        for index, segment_url in enumerate(segment_url_list):
            segment_path = pathlib.Path(get_segment_path_name(filename, index))
            with segment_path.open('rb') as segment_opened_file:
                opened_file.write(segment_opened_file.read())

    partial_file.rename(path)

    for index, segment_url in enumerate(segment_url_list):
        segment_path = pathlib.Path(get_segment_path_name(filename, index))
        os.remove(segment_path)

    return path.absolute()


def bit_rate(entry_data):
    return -int(entry_data['avg_bitrate'])


def process_video(video_data):
    video_name = video_data['name']
    video_url = video_data['url']

    if is_public_video(video_url):
        video_url = get_master_url_for_public_video(video_url)

    pathlib.Path('./combined').mkdir(parents=True, exist_ok=True)
    target_path = './combined/{}.mp4'.format(video_name)

    if pathlib.Path(target_path).exists():
        print("{} already exists, skipping".format(target_path))
        return

    print("Starting {}".format(video_name))

    with urllib.request.urlopen(video_url) as master_response:
        master_data = json.loads(master_response.read())

        video_data = sorted(master_data['video'], key=bit_rate)[0]
        audio_data = sorted(master_data['audio'], key=bit_rate)[0]

        video_base_url = urllib.parse.urljoin(urllib.parse.urljoin(video_url, master_data['base_url']),
                                              video_data['base_url'])
        audio_base_url = urllib.parse.urljoin(urllib.parse.urljoin(video_url, master_data['base_url']),
                                              audio_data['base_url'])

        video_path = process_file('video', video_base_url, video_data['init_segment'], video_data['segments'],
                                  video_name + '.m4v')
        audio_path = process_file('audio', audio_base_url, audio_data['init_segment'], audio_data['segments'],
                                  video_name + '.m4a')

        partial_target = pathlib.Path(target_path + '.~partial.mp4')
        if partial_target.exists():
            os.remove(partial_target)

        cmd_line = 'ffmpeg -y -loglevel quiet -i "{}" -i "{}" -c copy "{}"'.format(video_path, audio_path,
                                                                                   partial_target.absolute())

        print('Combining video and audio into {}'.format(target_path))
        os.system(cmd_line)
        partial_target.rename(target_path)

        os.remove(video_path)
        os.remove(audio_path)

        print('Completed {}'.format(target_path))


do_parallel([(lambda v=v: process_video(v)) for v in videoList], 2)
