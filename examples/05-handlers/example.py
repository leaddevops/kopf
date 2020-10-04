import kopf


# @kopf.on.resume('zalando.org', 'v1', 'kopfexamples')
# def resume_fn_1(**kwargs):
#     print(f'RESUMED 1st')


# @kopf.on.create('zalando.org', 'v1', 'kopfexamples')
# @kopf.on.create('zalando.org', 'kex')
# @kopf.on.create('zalando.org/v1', 'kex')
# @kopf.on.create('zalando.org/v2', 'kex')
# @kopf.on.create('zalando.org/v3', 'kex')
# @kopf.on.create('zalando.org', kopf.EVERYTHING)
# @kopf.on.create('kopfexamples.zalando.org')
# @kopf.on.create('kex.zalando.org')
@kopf.on.create(kopf.EVERYTHING)  # TERRIBLE IDEA! The cluster is overloaded in minutes (because of `kind: Event` self-creation).
# @kopf.on.create('kex')
# @kopf.on.create('KopfExample')
# @kopf.on.create(kind='KopfExample')
# @kopf.on.create(category='kopf')
# @kopf.on.create(category='all')
# @kopf.on.create('v1', 'pods')
# @kopf.on.resume('pods')
def create_fn_1(body, **kwargs):
    pass
    # print(f'CREATED 1st {body}')


# @kopf.on.resume('metrics.k8s.io', 'v1beta1', 'pods')
# def create_metric(body, **kwargs):
#     print(f'CREATED metric {body}')

# @kopf.on.resume('zalando.org', 'v1', 'kopfexamples')
# def resume_fn_2(**kwargs):
#     print(f'RESUMED 2nd')
#
#
# @kopf.on.create('zalando.org', 'v1', 'kopfexamples')
# def create_fn_2(**kwargs):
#     print('CREATED 2nd')
#
#
# @kopf.on.update('zalando.org', 'v1', 'kopfexamples')
# def update_fn(old, new, diff, **kwargs):
#     print('UPDATED')
#
#
# @kopf.on.delete('zalando.org', 'v1', 'kopfexamples')
# def delete_fn_1(**kwargs):
#     print('DELETED 1st')
#
#
# @kopf.on.delete('zalando.org', 'v1', 'kopfexamples')
# def delete_fn_2(**kwargs):
#     print('DELETED 2nd')
#
#
# @kopf.on.field('zalando.org', 'v1', 'kopfexamples', field='spec.field')
# def field_fn(old, new, **kwargs):
#     print(f'FIELD CHANGED: {old} -> {new}')
