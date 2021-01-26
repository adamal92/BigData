from firebase import Firebase
# https://bitbucket.org/joetilsed/firebase/src/master/firebase/__init__.py
config = {
    # "apiKey": "apiKey",
    # "authDomain": "projectId.firebaseapp.com",
    # "databaseURL": "https://databaseName.firebaseio.com",
    # "storageBucket": "projectId.appspot.com",
    # "serviceAccount": "path/to/serviceAccountCredentials.json"  # (optional)

    # "apiKey": "AIzaSyCOt619fNqEuIgFpzf20h2cmC6tFeQYuTE",
    # "authDomain": "corona-charts-33e8a.firebaseapp.com",
    # "databaseURL": "https://corona-charts-33e8a-default-rtdb.firebaseio.com/",
    # "storageBucket": "corona-charts-33e8a.appspot.com"

    "apiKey": "AIzaSyDxUQRW2HHezPGylz_bH-7Dh2NawCSDvkE",
    "authDomain": "rubyadam-df256.firebaseapp.com",
    "databaseURL": "https://rubyadam-df256.firebaseio.com",
    "projectId": "rubyadam-df256",
    "storageBucket": "rubyadam-df256.appspot.com",
    "messagingSenderId": "67384546234",
    "appId": "1:67384546234:web:53908c6a4a771ea3834383",
    "measurementId": "G-9TV8935QHL"

# <script src="https://www.gstatic.com/firebasejs/8.2.4/firebase-app.js"></script>
#
# <!-- TODO: Add SDKs for Firebase products that you want to use
#      https://firebase.google.com/docs/web/setup#available-libraries -->
# <script src="https://www.gstatic.com/firebasejs/8.2.4/firebase-analytics.js"></script>
#
# <script>
#   // Your web app's Firebase configuration
#   // For Firebase JS SDK v7.20.0 and later, measurementId is optional
#   var firebaseConfig = {
#     apiKey: "AIzaSyDxUQRW2HHezPGylz_bH-7Dh2NawCSDvkE",
#     authDomain: "rubyadam-df256.firebaseapp.com",
#     databaseURL: "https://rubyadam-df256.firebaseio.com",
#     projectId: "rubyadam-df256",
#     storageBucket: "rubyadam-df256.appspot.com",
#     messagingSenderId: "67384546234",
#     appId: "1:67384546234:web:53908c6a4a771ea3834383",
#     measurementId: "G-9TV8935QHL"
#   };
#   // Initialize Firebase
#   firebase.initializeApp(firebaseConfig);
#   firebase.analytics();
# </script>
}

firebase = Firebase(config)

db = firebase.database()
# print(db.child("users").get())

# users = db.child("users").get()
# print(users.val())
#
# user = db.child("users").get()
# print(user.key())
#
# users = db.child("Stats").get()
# print(users.val())
#
# # data = {"Corona": "my corona bitch"}
# # db.child("Stats").push(data)
#
# db.child("Stats").child("Corona").remove()
#
# users = db.child("Stats").get()
# print(users.val())
#
# all_user_ids = db.child("Stats").shallow().get()
# print(all_user_ids)
#
# users = db.child("Stats").get()
# print(users.val())
#
# # db.child("Stats").child("-MQmBJyn_Vy3GPrJHp_n").remove()
# # db.child("Stats").child("-MQmBPucWMGFVg7wW-v5").remove()
#
# data = {"Corona": "my corona"}
# db.child("Stats").push(["data", data])
# db.child("Stats").child("-MQmC_1BmrYYmpZFwSHZ").remove()
#
# db.child("Stats").push({"israel": "5885"})

# stats = db.child("Stats").get()
# print(stats.val())
# print(stats.key())
# print(stats)
# print(stats.each())
# print(stats.firebases())
# print(stats.query_key())

# stats = db.child("Stats").child({"israel_adam": "0"})
# stats_2 = db.get()
# print(stats_2.val())

# print(db.child("Stats").get().val())
# for stat in db.child("Stats").get().val().values():
#    stats_list.append(stat)
#    print(stat)

# # create database
# db.push({
#     "Stats": {
#         "cityIdRandom": {
#             "name": "city",
#             "code": 1,
#             "numOfSick": 20
#         }
#     }
# })

# print database
# recurse?
stats_list = []

for stat in db.get().val().values():
    stats_list.append(stat)
    print(stat)

print(db.get().val())
print(stats_list)

# for stat in db.get().val().values():
#     print(stat["Stats"]['cityIdRandom']['name'])
