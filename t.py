structorder = {'_id': 'STRUCT', 'cols': [['a', 'INT'], ['b', 'float'], ['KeyValue', 'a']]}
struct = structorder.copy()
acc = struct['cols']
del struct['cols']
for coll, typ in acc:
    struct[coll] = typ
print(struct)
l = []
newstring = ''
dict2 = {'a' : 'avalami', 'b': 'bvalami'}
for k, t in structorder['cols']:
    if k != 'KeyValue':
        l.append(dict2[k])
newstring = '#'.join(l)
print(newstring)
l2 = newstring.split('#')
i = 0
for k, t in structorder['cols']:
    if k != 'KeyValue':
        l2[i] = f'{k}:{l2[i]}'
        i += 1
newstring = '#'.join(l2)
print(newstring)
