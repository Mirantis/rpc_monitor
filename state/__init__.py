repositories_chain = set()


def handle_state_response(state_sample):
    for repo in repositories_chain:
        repo.on_state(state_sample)


def register_state_repository(repository):
    repositories_chain.add(repository)
