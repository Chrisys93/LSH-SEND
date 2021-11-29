# -*- coding: utf-8 -*-
"""Content placement strategies.

This module contains function to decide the allocation of content objects to
source nodes.

TODO: Work on the content generation and all the labels, characteristics and
    associated statistics.

"""
import random
import collections

from collections import Counter
from fnss.util import random_from_pdf
from icarus.registry import register_content_placement


__all__ = ['uniform_content_placement', 'uniform_repo_content_placement',
           'weighted_content_placement', 'weighted_repo_content_placement',
           'weighted_repo_bucket_placement', 'dataset_repo_bucket_placement']


def apply_content_placement(placement, topology):
    """Apply a placement to a topology

    Parameters
    ----------
    placement : dict of sets
        Set of contents to be assigned to nodes keyed by node identifier
    topology : Topology
        The topology
    """
    for v, contents in placement.items():
        topology.nodes[v]['stack'][1].update(contents = contents)

def apply_service_association(association, data):
    """
    Apply association of labels to contents

    Parameters
    ----------
    association:
    topics:
    types:
    :return:
    """
    for service_type, contents in association.items():
        for c in contents:
            if service_type not in data[c]['service_type']:
                data[c].update(service_type=service_type)
    return data

def apply_labels_association(association, data):
    """
    Apply association of labels to contents

    Parameters
    ----------
    association:
    topics:
    types:
    :return:
    """
    for label, contents in association.items():
        for c in contents:
            if label not in data[c]['labels']:
                data[c]['labels'].append(label)
    return data

def apply_space_association(association, data):
    """
    Apply association of labels to contents

    Parameters
    ----------
    association:
    topics:
    types:
    :return:
    """
    for h_space, contents in association.items():
        for c in contents:
            if h_space not in data[c]['h_space']:
                data[c]['h_space'].append(h_space)
    return data

def get_sources(topology):
    return [v for v in topology if topology.node[v]['stack'][0] == 'source']

def get_sources_repos(topology):
    try:
        return [v for v in topology if topology.node[v]['stack'][0] == 'source' or
                'source' and 'router' in topology.node[v]['extra-types']]
    except Exception as e:
        err_type = str(type(e)).split("'")[1].split(".")[1]
        if err_type == "KeyError":
            return [v for v in topology if topology.node[v]['stack'][0] == 'source']

@register_content_placement('UNIFORM')
def uniform_content_placement(topology, contents, seed=None):
    """Places content objects to source nodes randomly following a uniform
    distribution.

    Parameters
    ----------
    topology : Topology
        The topology object
   contents : iterable
        Iterable of content objects
    source_nodes : list
        List of nodes of the topology which are content sources

    Returns
    -------
    cache_placement : dict
        Dictionary mapping content objects to source nodes

    Notes
    -----
    A deterministic placement of objects (e.g., for reproducing results) can be
    achieved by using a fix seed value
    """
    random.seed(seed)
    source_nodes = get_sources(topology)
    content_placement = collections.defaultdict(set)
    for c in contents:
        content_placement[random.choice(source_nodes)].add(c)
    apply_content_placement(content_placement, topology)

@register_content_placement('UNIFORM_REPO')
def uniform_repo_content_placement(topology, contents, seed=None):
    """Places content objects to source nodes randomly following a uniform
    distribution.

    Parameters
    ----------
    topology : Topology
        The topology object
    contents : iterable
        Iterable of content objects
    source_nodes : list
        List of nodes of the topology which are content sources

    Returns
    -------
    cache_placement : dict
        Dictionary mapping content objects to source nodes

    Notes
    -----
    A deterministic placement of objects (e.g., for reproducing results) can be
    achieved by using a fix seed value
    """
    random.seed(seed)
    source_nodes = get_sources_repos(topology)
    content_placement = collections.defaultdict(dict)
    for c in contents:
        choice = random.choice(source_nodes)
        dict1 = {contents[c]['content']: c}
        content_placement[choice].update(dict1)
    apply_content_placement(content_placement, topology)


@register_content_placement('WEIGHTED')
def weighted_content_placement(topology, contents, source_weights, seed=None):
    """Places content objects to source nodes randomly according to the weight
    of the source node.

    Parameters
    ----------
    topology : Topology
        The topology object
   contents : iterable
        Iterable of content objects

   source_weights : dict
        Dict mapping nodes nodes of the topology which are content sources and
        the weight according to which content placement decision is made.

    Returns
    -------
    cache_placement : dict
        Dictionary mapping content objects to source nodes

    Notes
    -----
    A deterministic placement of objects (e.g., for reproducing results) can be
    achieved by using a fix seed value
    """
    random.seed(seed)
    norm_factor = float(sum(source_weights.values()))
    source_pdf = dict((k, v / norm_factor) for k, v in source_weights.items())
    content_placement = collections.defaultdict(set)
    for c in contents:
        content_placement[random_from_pdf(source_pdf)].add(c)
    apply_content_placement(content_placement, topology)


@register_content_placement('WEIGHTED_REPO')
def weighted_repo_content_placement(topology, contents, freshness_per, shelf_life, msg_size, topics_weights,
                                    types_weights, max_replications, source_weights, service_weights, max_label_nos,
                                    seed=None):
    """Places content objects to source nodes randomly according to the weight
    of the source node.

    TODO: This should be modified, or another one created, to include content
        placement parameters, like the freshness periods, shelf-lives, topics/types
        of labels and placement possibilities, maybe depending on hashes, placement
        of nodes and possibly other scenario-specific/service-specific parameters.
        ADD SERVICE TYPE TO MESSAGE PROPERTIES!

    Parameters
    ----------
    topology : Topology
        The topology object
    contents : iterable
        Iterable of content objects
    topics :

    types :

    freshness_per :

    shelf_life :

    msg_size :

    source_weights : dict
        Dict mapping nodes of the topology which are content sources and
        the weight according to which content placement decision is made.

    Returns
    -------
    cache_placement : dict
       Dictionary mapping content objects to source nodes

    Notes
    -----
    A deterministic placement of objects (e.g., for reproducing results) can be
    achieved by using a fix seed value
    """

    # TODO: This is the format that each datum (message) shuold have
    #       placed_data = {content, msg_topics, msg_type, freshness_per,
    #                       shelf_life, msg_size}

    placed_data = dict()
    random.seed(seed)
    norm_factor = float(sum(source_weights.values()))
    # TODO: These ^\/^\/^ might need redefining, to make label-specific
    #  source weights, and then the labels distributed according to these.
    #  OR the other way around, distributing sources according to label weights
    if types_weights is not None:
        types_labels_norm_factor = float(sum(types_weights.values()))
        types_labels_pdf = dict((k, v / types_labels_norm_factor) for k, v in types_weights.items())
    topics_labels_norm_factor = float(sum(topics_weights.values()))
    service_labels_norm_factor = float(sum(service_weights.values()))
    # TODO: Think about a way to randomise, but still maintain a certain
    #  distribution among the users that receive data with certain labels.
    #  Maybe associate the pdf with labels, rather than contents, SOMEHOW!
    source_pdf = dict((k, v / norm_factor) for k, v in source_weights.items())
    topics_labels_pdf = dict((k, v / topics_labels_norm_factor) for k, v in topics_weights.items())
    service_labels_pdf = dict((k, v / service_labels_norm_factor) for k, v in service_weights.items())
    service_association = collections.defaultdict(set)
    labels_association = collections.defaultdict(set)
    content_placement = collections.defaultdict(set)
    # Further TODO: Add all the other data characteristics and maybe place
    #           content depending on those at a later point (create other
    #           placement strategies)
    # NOTE: All label names will come as a list of strings
    for c in contents:
        alter = False
        if freshness_per is not None:
            if placed_data.has_key(contents[c]['content']):
                placed_data[contents[c]['content']].update(freshness_per=freshness_per)
            else:
                placed_data[contents[c]['content']] = dict()
                placed_data[contents[c]['content']]['freshness_per'] = freshness_per
        if shelf_life is not None:
            placed_data[contents[c]['content']].update(shelf_life=shelf_life)
        if max_replications:
            placed_data[contents[c]['content']].update(max_replications=max_replications)
            placed_data[contents[c]['content']].update(replications=0)
        service_association[random_from_pdf(service_labels_pdf)].add(c)
        placed_data[contents[c]['content']].update(content=c)
        placed_data[contents[c]['content']].update(msg_size=msg_size)
        placed_data[contents[c]['content']]["receiveTime"] = 0
        placed_data[contents[c]['content']]['labels'] = contents[c]['labels']
        placed_data[contents[c]['content']]['service_type'] = "non-proc"
        if not placed_data[contents[c]['content']]['labels']:
            for i in range(0, max_label_nos):
                if types_weights is not None and not alter:
                    labels_association[random_from_pdf(types_labels_pdf)].add(c)
                    alter = True
                elif topics_weights is not None and alter:
                    labels_association[random_from_pdf(topics_labels_pdf)].add(c)
                    alter = False
                elif topics_weights is not None:
                    labels_association[random_from_pdf(topics_labels_pdf)].add(c)

    placed_data = apply_labels_association(labels_association, placed_data)
    #placed_data = apply_service_association(service_association, placed_data)
    for d in placed_data:
        rand = random_from_pdf(source_pdf)
        if not content_placement[rand]:
            content_placement[rand] = dict()
        if content_placement[rand].has_key(d):
            content_placement[rand][d].update(placed_data[d])
        else:
            content_placement[rand][d] = dict()
            content_placement[rand][d] = placed_data[d]
    apply_content_placement(content_placement, topology)
    topology.placed_data = placed_data

# TODO: Recreate the below, for inherited hashes, from the workload, and to place hashes (as it does already), but more
#  randomly - possibly with a general distribution, mentioned in the config file, like in the workload

@register_content_placement('WEIGHTED_REPO_HASH')
def weighted_repo_hash_placement(topology, contents, freshness_per, shelf_life, msg_size, topics_weights,
                                 types_weights, space_weights, max_replications, source_weights, service_weights,
                                 max_label_nos, seed=None):
    """Places content objects to source nodes randomly according to the weight
    of the source node.

    TODO: This should be modified, or another one created, to include content
        placement parameters, like the freshness periods, shelf-lives, topics/types
        of labels and placement possibilities, maybe depending on hashes, placement
        of nodes and possibly other scenario-specific/service-specific parameters.
        ADD SERVICE TYPE TO MESSAGE PROPERTIES!

    Parameters
    ----------
    topology : Topology
        The topology object
    contents : iterable
        Iterable of content objects
    topics :

    types :

    freshness_per :

    shelf_life :

    msg_size :

    source_weights : dict
        Dict mapping nodes of the topology which are content sources and
        the weight according to which content placement decision is made.

    Returns
    -------
    cache_placement : dict
       Dictionary mapping content objects to source node
    service_labels_norm_factor = float(sum(service_weights.values()))s

    Notes
    -----
    A deterministic placement of objects (e.g., for reproducing results) can be
    achieved by using a fix seed value
    """

    # TODO: This is the format that each datum (message) shuold have
    #       placed_data = {content, msg_topics, msg_type, freshness_per,
    #                       shelf_life, msg_size}

    placed_data = dict()
    random.seed(seed)
    norm_factor = float(sum(source_weights.values()))
    # TODO: These ^\/^\/^ might need redefining, to make label-specific
    #  source weights, and then the labels distributed according to these.
    #  OR the other way around, distributing sources according to label weights
    if types_weights is not None:
        types_labels_norm_factor = float(sum(types_weights.values()))
        types_labels_pdf = dict((k, v / types_labels_norm_factor) for k, v in types_weights.items())
    else: alter = True
    topics_labels_norm_factor = float(sum(topics_weights.values()))
    space_norm_factor = float(sum(space_weights.values()))
    service_labels_norm_factor = float(sum(service_weights.values()))
    # TODO: Think about a way to randomise, but still maintain a certain
    #  distribution among the users that receive data with certain labels.
    #  Maybe associate the pdf with labels, rather than contents, SOMEHOW!
    source_pdf = dict((k, v / norm_factor) for k, v in source_weights.items())
    space_pdf = dict((k, v / space_norm_factor) for k, v in space_weights.items())
    topics_labels_pdf = dict((k, v / topics_labels_norm_factor) for k, v in topics_weights.items())
    service_labels_pdf = dict((k, v / service_labels_norm_factor) for k, v in service_weights.items())
    service_association = collections.defaultdict(set)
    labels_association = collections.defaultdict(set)
    space_association = collections.defaultdict(set)
    content_placement = collections.defaultdict(set)
    # Further TODO: Add all the other data characteristics and maybe place
    #           content depending on those at a later point (create other
    #           placement strategies)
    # NOTE: All label names will come as a list of strings
    for c in contents:
        if alter:
            alter = False
        else:
            alter = None
        if freshness_per is not None:
            if placed_data.has_key(contents[c]['content']):
                placed_data[contents[c]['content']].update(freshness_per=freshness_per)
            else:
                placed_data[contents[c]['content']] = dict()
                placed_data[contents[c]['content']]['freshness_per'] = freshness_per
        if shelf_life is not None:
            placed_data[contents[c]['content']].update(shelf_life=shelf_life)
        if max_replications:
            placed_data[contents[c]['content']].update(max_replications=max_replications)
            placed_data[contents[c]['content']].update(replications=0)
        service_association[random_from_pdf(service_labels_pdf)].add(c)
        placed_data[contents[c]['content']].update(content=c)
        placed_data[contents[c]['content']].update(msg_size=msg_size)
        placed_data[contents[c]['content']]["receiveTime"] = 0
        placed_data[contents[c]['content']]['labels'] = contents[c]['labels']
        placed_data[contents[c]['content']]['h_space'] = contents[c]['h_space']
        placed_data[contents[c]['content']]['service_type'] = "non-proc"
        if not placed_data[contents[c]['content']]['h_space']:
            for i in range(0, max_label_nos):
                if space_weights is not None:
                    space_association[random_from_pdf(space_pdf)].add(c)
        if not placed_data[contents[c]['content']]['labels']:
            for i in range(0, max_label_nos):
                if types_weights is not None and alter is not None and not alter:
                    labels_association[random_from_pdf(types_labels_pdf)].add(c)
                    alter = True
                elif topics_weights is not None and alter is not None and alter:
                    labels_association[random_from_pdf(topics_labels_pdf)].add(c)
                    alter = False
                elif topics_weights is not None:
                    labels_association[random_from_pdf(topics_labels_pdf)].add(c)

    placed_data = apply_space_association(space_association, placed_data)

    placed_data = apply_labels_association(labels_association, placed_data)
    #placed_data = apply_service_association(service_association, placed_data)
    # FIXME: HERE is where data should be randomly distributed via hash-space!
    #  (Possibly - need to re-check) FIXED!
    content_h_space = dict()
    for d in placed_data:
        rand = random_from_pdf(source_pdf)
        if not content_placement[rand]:
            content_placement[rand] = dict()
            content_h_space[rand] = []
            content_h_space[rand].append(placed_data[d]['h_space'])
        elif placed_data[d]['h_space'] not in content_h_space[rand] and len(content_h_space[rand]) < 10:
            content_h_space[rand].append(placed_data[d]['h_space'])
        if content_placement[rand].has_key(d) and placed_data[d]['h_space'] in content_h_space[rand]:
            content_placement[rand][d].update(placed_data[d])
        elif placed_data[d]['h_space'] in content_h_space[rand]:
            content_placement[rand][d] = dict()
            content_placement[rand][d] = placed_data[d]
        else:
            for node in content_h_space:
                if placed_data[d]['h_space'] in content_h_space[node]:
                    if content_placement[node].has_key(d):
                        content_placement[node][d].update(placed_data[d])
                    elif placed_data[d]['h_space'] in content_h_space[node]:
                        content_placement[node][d] = dict()
                        content_placement[node][d] = placed_data[d]
    apply_content_placement(content_placement, topology)
    topology.placed_data = placed_data

@register_content_placement('WEIGHTED_BUCKET_REPO_HASH')
def weighted_repo_bucket_placement(topology, contents, freshness_per, shelf_life, msg_size, topics_weights,
                                 types_weights, space_weights, max_replications, source_weights, service_weights,
                                 max_label_nos, seed=None):
    """Places content objects to source nodes randomly according to the weight
    of the source node.

    TODO: This should be modified, or another one created, to include content
        placement parameters, like the freshness periods, shelf-lives, topics/types
        of labels and placement possibilities, maybe depending on hashes, placement
        of nodes and possibly other scenario-specific/service-specific parameters.
        ADD SERVICE TYPE TO MESSAGE PROPERTIES!

    Parameters
    ----------
    topology : Topology
        The topology object
    contents : iterable
        Iterable of content objects
    topics :

    types :

    freshness_per :

    shelf_life :

    msg_size :

    source_weights : dict
        Dict mapping nodes of the topology which are content sources and
        the weight according to which content placement decision is made.

    Returns
    -------
    cache_placement : dict
       Dictionary mapping content objects to source node
    service_labels_norm_factor = float(sum(service_weights.values()))s

    Notes
    -----
    A deterministic placement of objects (e.g., for reproducing results) can be
    achieved by using a fix seed value
    """

    # TODO: This is the format that each datum (message) shuold have
    #       placed_data = {content, msg_topics, msg_type, freshness_per,
    #                       shelf_life, msg_size}

    placed_data = dict()
    random.seed(seed)
    norm_factor = float(sum(source_weights.values()))
    # TODO: These ^\/^\/^ might need redefining, to make label-specific
    #  source weights, and then the labels distributed according to these.
    #  OR the other way around, distributing sources according to label weights
    if types_weights is not None:
        types_labels_norm_factor = float(sum(types_weights.values()))
        types_labels_pdf = dict((k, v / types_labels_norm_factor) for k, v in types_weights.items())
    else: alter = True
    topics_labels_norm_factor = float(sum(topics_weights.values()))
    space_norm_factor = float(sum(space_weights.values()))
    service_labels_norm_factor = float(sum(service_weights.values()))
    # TODO: Think about a way to randomise, but still maintain a certain
    #  distribution among the users that receive data with certain labels.
    #  Maybe associate the pdf with labels, rather than contents, SOMEHOW!
    source_pdf = dict((k, v / norm_factor) for k, v in source_weights.items())
    space_pdf = dict((k, v / space_norm_factor) for k, v in space_weights.items())
    topics_labels_pdf = dict((k, v / topics_labels_norm_factor) for k, v in topics_weights.items())
    service_labels_pdf = dict((k, v / service_labels_norm_factor) for k, v in service_weights.items())
    service_association = collections.defaultdict(set)
    labels_association = collections.defaultdict(set)
    space_association = collections.defaultdict(set)
    content_placement = collections.defaultdict(set)
    # Further TODO: Add all the other data characteristics and maybe place
    #           content depending on those at a later point (create other
    #           placement strategies)
    # NOTE: All label names will come as a list of strings
    for c in contents:
        if alter:
            alter = False
        else:
            alter = None
        if freshness_per is not None:
            if placed_data.has_key(contents[c]['content']):
                placed_data[contents[c]['content']].update(freshness_per=freshness_per)
            else:
                placed_data[contents[c]['content']] = dict()
                placed_data[contents[c]['content']]['freshness_per'] = freshness_per
        if shelf_life is not None:
            placed_data[contents[c]['content']].update(shelf_life=shelf_life)
        if max_replications:
            placed_data[contents[c]['content']].update(max_replications=max_replications)
            placed_data[contents[c]['content']].update(replications=0)
        service_association[random_from_pdf(service_labels_pdf)].add(c)
        placed_data[contents[c]['content']].update(content=c)
        placed_data[contents[c]['content']].update(msg_size=msg_size)
        placed_data[contents[c]['content']]["receiveTime"] = 0
        placed_data[contents[c]['content']]['labels'] = contents[c]['labels']
        placed_data[contents[c]['content']]['h_space'] = contents[c]['h_space']
        placed_data[contents[c]['content']]['service_type'] = "non-proc"
        if not placed_data[contents[c]['content']]['h_space']:
            for i in range(0, max_label_nos):
                if space_weights is not None:
                    space_association[random_from_pdf(space_pdf)].add(c)
        if not placed_data[contents[c]['content']]['labels']:
            for i in range(0, max_label_nos):
                if types_weights is not None and alter is not None and not alter:
                    labels_association[random_from_pdf(types_labels_pdf)].add(c)
                    alter = True
                elif topics_weights is not None and alter is not None and alter:
                    labels_association[random_from_pdf(topics_labels_pdf)].add(c)
                    alter = False
                elif topics_weights is not None:
                    labels_association[random_from_pdf(topics_labels_pdf)].add(c)

    placed_data = apply_space_association(space_association, placed_data)

    placed_data = apply_labels_association(labels_association, placed_data)
    #placed_data = apply_service_association(service_association, placed_data)
    # FIXME: HERE is where data should be randomly distributed via hash-space!
    #  (Possibly - need to re-check) FIXED!


    content_h_space = dict()
    h_spaces = []
    for d in placed_data:
        rand = random_from_pdf(source_pdf)
        if rand not in content_h_space:
            content_h_space[rand] = Counter()
        content_h_space[rand].update(placed_data[d]['h_space'])
    for n in content_h_space:
        for h in content_h_space[n]:
            if h not in h_spaces:
                h_spaces.append(h)

    max_occ = 0
    max_h = []
    max_node = 0
    placement_h_space = dict()
    for h in h_spaces:
        max_occ = 0
        for node in content_h_space:
            if content_h_space[node][h] > max_occ:
                max_occ = content_h_space[node][h]
                max_h = h
                max_node = node
        if max_node not in placement_h_space:
            placement_h_space[max_node] = []
        placement_h_space[max_node].append(max_h)
    for node in placement_h_space:
        content_h_space[node] = placement_h_space[node]
    for node in content_h_space:
        if type(content_h_space[node]) is Counter:
            content_h_space[node] = []

    # FIXME: The following is not really scalable to more than one bucket association per content

    for n in content_h_space:
        for d in placed_data:
            for bucket in content_h_space[n]:
                if placed_data[d]['h_space'][0] == bucket:
                    if n not in content_placement:
                        content_placement[n] = dict()
                    if content_placement[n].has_key(d):
                        content_placement[n][d].update(placed_data[d])
                    else:
                        content_placement[n][d] = dict()
                        content_placement[n][d] = placed_data[d]

    apply_content_placement(content_placement, topology)
    topology.placed_data = placed_data


@register_content_placement('DATASET_BUCKET_REPO_HASH')
def dataset_repo_bucket_placement(topology, contents, freshness_per, shelf_life, msg_size, topics_weights,
                                 types_weights, max_replications, source_weights, service_weights,
                                 max_label_nos, seed=None):
    """Places content objects to source nodes randomly according to the weight
    of the source node.

    TODO: This should be modified, or another one created, to include content
        placement parameters, like the freshness periods, shelf-lives, topics/types
        of labels and placement possibilities, maybe depending on hashes, placement
        of nodes and possibly other scenario-specific/service-specific parameters.
        ADD SERVICE TYPE TO MESSAGE PROPERTIES!

    Parameters
    ----------
    topology : Topology
        The topology object
    contents : iterable
        Iterable of content objects
    topics :

    types :

    freshness_per :

    shelf_life :

    msg_size :

    source_weights : dict
        Dict mapping nodes of the topology which are content sources and
        the weight according to which content placement decision is made.

    Returns
    -------
    cache_placement : dict
       Dictionary mapping content objects to source node
    service_labels_norm_factor = float(sum(service_weights.values()))s

    Notes
    -----
    A deterministic placement of objects (e.g., for reproducing results) can be
    achieved by using a fix seed value
    """

    # TODO: This is the format that each datum (message) shuold have
    #       placed_data = {content, msg_topics, msg_type, freshness_per,
    #                       shelf_life, msg_size}

    placed_data = dict()
    random.seed(seed)
    norm_factor = float(sum(source_weights.values()))
    # TODO: These ^\/^\/^ might need redefining, to make label-specific
    #  source weights, and then the labels distributed according to these.
    #  OR the other way around, distributing sources according to label weights
    if types_weights is not None:
        types_labels_norm_factor = float(sum(types_weights.values()))
        types_labels_pdf = dict((k, v / types_labels_norm_factor) for k, v in types_weights.items())
    else: alter = True
    topics_labels_norm_factor = float(sum(topics_weights.values()))
    space_norm_factor = float(sum(space_weights.values()))
    service_labels_norm_factor = float(sum(service_weights.values()))
    # TODO: Think about a way to randomise, but still maintain a certain
    #  distribution among the users that receive data with certain labels.
    #  Maybe associate the pdf with labels, rather than contents, SOMEHOW!
    source_pdf = dict((k, v / norm_factor) for k, v in source_weights.items())
    space_pdf = dict((k, v / space_norm_factor) for k, v in space_weights.items())
    topics_labels_pdf = dict((k, v / topics_labels_norm_factor) for k, v in topics_weights.items())
    service_labels_pdf = dict((k, v / service_labels_norm_factor) for k, v in service_weights.items())
    service_association = collections.defaultdict(set)
    labels_association = collections.defaultdict(set)
    space_association = collections.defaultdict(set)
    content_placement = collections.defaultdict(set)
    # Further TODO: Add all the other data characteristics and maybe place
    #           content depending on those at a later point (create other
    #           placement strategies)
    # NOTE: All label names will come as a list of strings
    for c in contents:
        if alter:
            alter = False
        else:
            alter = None
        if freshness_per is not None:
            if placed_data.has_key(contents[c]['content']):
                placed_data[contents[c]['content']].update(freshness_per=freshness_per)
            else:
                placed_data[contents[c]['content']] = dict()
                placed_data[contents[c]['content']]['freshness_per'] = freshness_per
        if shelf_life is not None:
            placed_data[contents[c]['content']].update(shelf_life=shelf_life)
        if max_replications:
            placed_data[contents[c]['content']].update(max_replications=max_replications)
            placed_data[contents[c]['content']].update(replications=0)
        service_association[random_from_pdf(service_labels_pdf)].add(c)
        placed_data[contents[c]['content']].update(content=c)
        placed_data[contents[c]['content']].update(msg_size=msg_size)
        placed_data[contents[c]['content']]["receiveTime"] = 0
        placed_data[contents[c]['content']]['labels'] = contents[c]['labels']
        placed_data[contents[c]['content']]['h_space'] = contents[c]['h_space']
        placed_data[contents[c]['content']]['service_type'] = "non-proc"
        if not placed_data[contents[c]['content']]['labels']:
            for i in range(0, max_label_nos):
                if types_weights is not None and alter is not None and not alter:
                    labels_association[random_from_pdf(types_labels_pdf)].add(c)
                    alter = True
                elif topics_weights is not None and alter is not None and alter:
                    labels_association[random_from_pdf(topics_labels_pdf)].add(c)
                    alter = False
                elif topics_weights is not None:
                    labels_association[random_from_pdf(topics_labels_pdf)].add(c)

    placed_data = apply_space_association(space_association, placed_data)

    placed_data = apply_labels_association(labels_association, placed_data)
    #placed_data = apply_service_association(service_association, placed_data)
    # FIXME: HERE is where data should be randomly distributed via hash-space!
    #  (Possibly - need to re-check) FIXED!


    content_h_space = dict()
    h_spaces = []
    for d in placed_data:
        rand = random_from_pdf(source_pdf)
        if rand not in content_h_space:
            content_h_space[rand] = Counter()
        content_h_space[rand].update(placed_data[d]['h_space'])
    for n in content_h_space:
        for h in content_h_space[n]:
            if h not in h_spaces:
                h_spaces.append(h)

    max_occ = 0
    max_h = []
    max_node = 0
    placement_h_space = dict()
    for h in h_spaces:
        max_occ = 0
        for node in content_h_space:
            if content_h_space[node][h] > max_occ:
                max_occ = content_h_space[node][h]
                max_h = h
                max_node = node
        if max_node not in placement_h_space:
            placement_h_space[max_node] = []
        placement_h_space[max_node].append(max_h)
    for node in placement_h_space:
        content_h_space[node] = placement_h_space[node]
    for node in content_h_space:
        if type(content_h_space[node]) is Counter:
            content_h_space[node] = []

    # FIXME: The following is not really scalable to more than one bucket association per content

    for n in content_h_space:
        for d in placed_data:
            for bucket in content_h_space[n]:
                if placed_data[d]['h_space'][0] == bucket:
                    if n not in content_placement:
                        content_placement[n] = dict()
                    if content_placement[n].has_key(d):
                        content_placement[n][d].update(placed_data[d])
                    else:
                        content_placement[n][d] = dict()
                        content_placement[n][d] = placed_data[d]

    apply_content_placement(content_placement, topology)
    topology.placed_data = placed_data



