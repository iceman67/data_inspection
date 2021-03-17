from tasks import summary
import json

result = summary.delay()
res = json.loads(result.get())
for i in res:
    print(i)

