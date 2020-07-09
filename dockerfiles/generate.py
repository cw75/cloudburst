import cloudpickle as cp
import numpy as np

users = []
for i in range(1000):
	users.append('user_' + str(i).zfill(3))

follow = {}

for i in range(1000):
	uid = 'user_' + str(i).zfill(3)
	users_copy = users.copy()
	users_copy.remove(uid)
	follow_target = np.random.choice(users_copy, size=50, replace=False).tolist()
	follow[uid] = follow_target

with open('graph', "wb") as f:
	cp.dump((users, follow), f)

with open('graph', "rb") as f:
	u, f = cp.load(f)
	print(f)
	print(u)