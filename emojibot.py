from cloudburst.client.client import CloudburstConnection

cloudburst = CloudburstConnection('xxx', 'xxx')

class EmojiBot:
  import logging
  import json
  import numpy as np
  from torchmoji.sentence_tokenizer import SentenceTokenizer
  from torchmoji.model_def import torchmoji_emojis
  from slack import WebClient
  from slack.errors import SlackApiError

  def __init__(self, user_library, tk):
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
                  :sparkling_heart: :blue_heart: :grimacing: :sparkles:".split()

  def run(self, user_library, event):
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
        logging.info('max prob is %s' % max(prob))
        if max(prob) > 0.1:
          ind = self.np.argpartition(prob, -1)[-1:]
          emoji_ids = ind[self.np.argsort(prob[ind])][::-1]
          emojis = list(map(lambda x: self.EMOJIS[x].strip(':'), emoji_ids))

          logging.info(emojis[0])

          response = self.slack_web_client.reactions_add(channel=channel_id,name=emojis[0],timestamp=ts)
          self.logging.info(response)
          return response
        else:
          self.logging.info('low confidence')
          return 'low confidence'
    except self.SlackApiError as e:
        self.logging.error(e.response)
        return e.response

cloudburst.register_slack_bot(EmojiBot, 'xxx', 'xxx')