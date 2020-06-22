from cloudburst.client.client import CloudburstConnection

cloudburst = CloudburstConnection('a75452a85ab9d408696cea569c8cf0b6-1105309470.us-east-1.elb.amazonaws.com', '18.213.253.255')

class Test:
  from slack import WebClient
  from slack.errors import SlackApiError
  import logging
  import json
  from torchmoji.sentence_tokenizer import SentenceTokenizer
  from torchmoji.model_def import torchmoji_emojis
  import numpy as np
  def __init__(self, _, tk):
    self.slack_web_client = self.WebClient(token=tk)
    with open('/vocabulary.json', 'r') as f:
        vocabulary = self.json.load(f)
    self.st = self.SentenceTokenizer(vocabulary, 100)
    self.m = self.torchmoji_emojis('/pytorch_model.bin')
    self.EMOJIS = ":joy: :unamused: :weary: :sob: :heart_eyes: \
:pensive: :ok_hand: :blush: :heart: :smirk: \
:grin: :notes: :flushed: :100: :sleeping: \
:relieved: :relaxed: :raised_hands: :two_hearts: :expressionless: \
:sweat_smile: :pray: :confused: :kissing_heart: :heartbeat: \
:neutral_face: :information_desk_person: :disappointed: :see_no_evil: :tired_face: \
:v: :sunglasses: :rage: :thumbsup: :cry: \
:sleepy: :yum: :triumph: :hand: :mask: \
:clap: :eyes: :gun: :persevere: :smiling_imp: \
:sweat: :broken_heart: :yellow_heart: :musical_note: :speak_no_evil: \
:wink: :skull: :confounded: :smile: :stuck_out_tongue_winking_eye: \
:angry: :no_good: :muscle: :facepunch: :purple_heart: \
:sparkling_heart: :blue_heart: :grimacing: :sparkles:".split(' ')

  def run(self, _, event):
    try:
        channel_id = event['channel']
        user_id = event['user']
        text = event['text']
        ts = event['ts']

        self.logging.info('text is ' + text)
        if not isinstance(text, list):
            text = [text]
        tokenized, _, _ = self.st.tokenize_sentences(text)
        prob = self.m(tokenized)[0]
        ind = self.np.argpartition(prob, -1)[-1:]
        emoji_ids = ind[self.np.argsort(prob[ind])][::-1]
        emojis = list(map(lambda x: self.EMOJIS[x].strip(':'), emoji_ids))

        response = self.slack_web_client.reactions_add(channel=channel_id,name=emojis[0],timestamp=ts)
        self.logging.info(response)
        return response
    except self.SlackApiError as e:
        self.logging.error(e.response)
        return e.response

print('registering test function')
cloudburst.register((Test, ('xoxb-1189572766630-1196518007843-ikhRwzpjrMZzRa6FBJSgfV1h',)), 'test')
print('registered test function')

print('registering bot A015SF4MFA7')
cloudburst.register_dag('A015SF4MFA7', ['test'], [])
print('registered')

#print('calling test dag')
#print(cloudburst.call_dag('test_dag', { 'test': 'hello' }, direct_response=True))