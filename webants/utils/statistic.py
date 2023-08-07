from pygather.libs import Request


# Statistic = namedtuple('Statistic',
#                        ['url',
#                         'next_url',
#                         'status',
#                         'exception',
#                         'size',
#                         'content_type',
#                         'encoding',
#                         'num_urls',
#                         'num_new_urls'],
#                        defaults=[None, None, None, None, None, None, None, None, None, ])
class Statistic:
    request: Request
    next_requests: list[Request]

    def __init__(self):
        pass


if __name__ == '__main__':
    s = Statistic()
    print(s)
