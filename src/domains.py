"""Transparent keyword grouping for demonstration, not an NLP classifier."""
GROUPS = {
    'Healthcare': ['health', 'hospital', 'doctor', 'vaccine', 'care', 'covid', 'mask', 'nurse', 'medical'],
    'Politics': ['politic', 'election', 'obama', 'congress', 'parliament', 'democracy', 'president', 'bjp', 'modi', 'trump', 'hongkong', 'china', 'maga', 'impeach', 'cdnpoli'],
    'Sports': ['cricket', 'football', 'soccer', 'tennis', 'sports', 'fifa', 'ipl', 'nba', 'nfl', 'rugby', 'olympic'],
    'Technology': ['technology', 'tech', 'python', 'android', 'iphone', 'apple', 'google', 'microsoft', 'robot', 'artificialintelligence', 'machinelearning'],
    'Entertainment': ['bollywood', 'hollywood', 'movie', 'music', 'cinema', 'beyonce', 'bieber', 'grammy', 'oscars', 'nowplaying', 'concert', 'masked']}


def domain_for(tag):
    value = tag.lstrip('#').casefold()
    if value in {'ai', 'ml', 'iot', 'chatgpt'}:
        return 'Technology'
    for domain, keywords in GROUPS.items():
        if any(word in value for word in keywords):
            return domain
    return 'Other'
