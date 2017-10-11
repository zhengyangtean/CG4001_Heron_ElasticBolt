import sys

input="""
0 :: d!!!=467, 1 :: b!!!=572, 0 :: b!!!=685, 2 :: b!!!=743, 0 :: f!!!=495, 0 ::
h!!!=588, 2 :: j!!!=1031, 2 :: h!!!=713, 0 :: j!!!=414, 2 :: d!!!=754, 2 ::
f!!!=967, 1 :: d!!!=779, 1 :: j!!!=555, 1 :: h!!!=699, 1 :: f!!!=538, 1 ::
a!!!=627, 0 :: e!!!=795, 0 :: c!!!=506, 0 :: g!!!=620, 2 :: c!!!=953, 2 ::
a!!!=541, 2 :: i!!!=745, 2 :: g!!!=758, 1 :: g!!!=622, 0 :: i!!!=621, 2 ::
e!!!=741, 0 :: a!!!=832, 1 :: i!!!=634, 1 :: c!!!=541, 1 :: e!!!=464
"""


input = input.replace("\n","").replace(" ", "")

segments = (input.split(','))

d = {}

for thread in sorted(segments):
    print(thread)
    subthread = thread.split('!!!=')
    if subthread[0][0] not in d:
        d[subthread[0][0]] = 0
    d[subthread[0][0]] += int(subthread[1])
print(d)


