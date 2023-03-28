"""
Recommendation engine for the songs in Neo4j database.
"""

import neo4j

class RecommendationEngine():
    def __init__(self, uri, user, password):
        self._driver = neo4j.GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        """
        Close the driver connection.
        """

        self._driver.close()

    def clear_database(self):
        """
        Clear the database.
        """

        with self._driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("Database cleared.")

    def insert_nodes_and_relationships(self):
        """
        Insert nodes and relationships from csv files into Neo4j database.

        GRAPH SCHEMA:
        Nodes are given properties: song_idx, artist, album, name, popularity, genre
        Relationships are given properties: similarity
        """

        # cypher query to insert nodes from nodes.csv
        node_query = """
        LOAD CSV WITH HEADERS FROM "file:///Users/sree/Second2/DS4300/assignments/song_recommendation_engine/nodes.csv" AS row
        CREATE (n:Song {song_idx: row.song_idx, artist: row.artists, album: row.album_name, name: row.track_name, popularity: row.popularity, genre: row.track_genre})
        """

        # cypher query to insert relationships from edges.csv where undirected edge from source to target is represented with similarity property
        # edge has properties: similarity
        edges_query = """
        LOAD CSV WITH HEADERS FROM "file:///Users/sree/Second2/DS4300/assignments/song_recommendation_engine/edges.csv" AS row
        MATCH (s:Song {song_idx: row.source}), (t:Song {song_idx: row.target})
        MERGE (s)-[r:SIMILAR_TO {similarity: row.similarity}]-(t)
        RETURN COUNT(r)
        """

        with self._driver.session() as session:
            session.run(node_query)
            print("Nodes inserted.")

            result = session.run(edges_query)
            for record in result:
                print("Relationships inserted:", record[0])

            print("Nodes and relationships inserted.")

    def list_nodes(self):
        """
        List all nodes in the graph.
        """

        query = """
        MATCH (n)
        RETURN n
        """

        with self._driver.session() as session:
            result = session.run(query)
            for record in result:
                print(record)

    def count_relationships(self):
        """
        Count number of relationships in the graph.
        """

        query = """
        MATCH ()-[r]->()
        RETURN COUNT(r)
        """

        with self._driver.session() as session:
            result = session.run(query)
            for record in result:
                print(record)

    def recommend_songs(self, song_name, artist_name, limit=10):
        """
        Recommend songs based on similarity between given song and other songs.

        RECOMMENDATION ALGORITHM:\ 
        Recommend songs based on similarity between given song and other songs.\ 
        Check if song was made by same artist and has same album as given song.\ 
        Include recommendations only if popularity of recommended song is less than or equal to popularity of given song.\ 
        Return top songs based on similarity property of relationship.

        :param song_name: name of song
        :type song_name: str
        :param artist_name: name of artist
        :type artist_name: str
        :param limit: limit of top recommendations to return, defaults to 10
        :type limit: int, optional
        :return recommendations: list of recommendations
        :rtype: list
        """

        # cypher query to recommend songs based on similarity between given song and other songs
        # return top 5 songs based on similarity property of relationship
        # song not recommended if it has same album as given song
        query = """
        MATCH (s:Song {name: $song_name, artist: $artist_name})-[r:SIMILAR_TO]-(t:Song)
        WHERE s.album <> t.album AND s.artist <> t.artist 
        AND toInteger(t.popularity) <= toInteger(s.popularity)
        RETURN t.name, t.artist, t.popularity, r.similarity AS similarity_score
        ORDER BY similarity_score DESC
        LIMIT $limit
        """

        with self._driver.session() as session:
            result = session.run(query, song_name=str(song_name), artist_name=str(artist_name), limit=limit)

            # print("Recommendations for:", song_name, "by", artist_name, "\n")
            recommendations = []
            for record in result:
                recommendations.append((record['t.name'], record['t.artist'], record['similarity_score']))
                # print("Song:", record['t.name'], "\tArtist:", record['t.artist'], "\tSimilarity Score:", record['similarity_score'], "\n")

            return recommendations
    
    def recommend_from_multiple_songs(self, name_list, artist_list, limit=10):
        """
        Recommend songs based on similarity between given songs and other songs.\ 
        Combine recommendations from all songs and return top recommendations across all songs.

        :param name_list: list of song names
        :type name_list: list
        :param artist_list: list of artists
        :type artist_list: list
        :param limit: limit of top recommendations to return, defaults to 10
        :type limit: int, optional
        """
        
        recommendations = []
        for name, artist in zip(name_list, artist_list):
            recommendations.append(self.recommend_songs(name, artist, limit))

        # recommendations with highest similarity score out of all recommendations
        top_recommendations = []
        for i in range(limit):
            max_score = 0
            max_recommendation = None
            for recommendation in recommendations:
                if len(recommendation) > i and float(recommendation[i][2]) > max_score:
                    # check if song name is already in top recommendations
                    if recommendation[i][0] in [record[0] for record in top_recommendations]:
                        continue

                    max_score = float(recommendation[i][2])
                    max_recommendation = recommendation[i]
            if max_recommendation:
                top_recommendations.append(max_recommendation)

        # print recommendations
        print("Recommendations for:", name_list, "by", artist_list, "\n")
        for record in top_recommendations:
            print("Song:", record[0], "\tArtist:", record[1], "\tSimilarity Score:", record[2], "\n")

    def get_relationships(self, song_idx):
        """
        Get all relationships for a given song.

        :param song_idx: index of song
        :type song_idx: int
        """

        # cypher query to get all relationships for a given song
        query = """
        MATCH (s:Song {song_idx: $song_idx})-[r:SIMILAR_TO]-(t:Song)
        RETURN t.name, t.artist, r.similarity
        ORDER BY r.similarity DESC
        """

        with self._driver.session() as session:
            result = session.run(query, song_idx=str(song_idx))
            for record in result:
                print(record)

    def show_graph(self):
        """
        Show the graph in Neo4j database.
        """

       # display all nodes and relationships in the graph
        query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]-()
        RETURN n, type(r), r
        """

        with self._driver.session() as session:
            result = session.run(query)
            for record in result:
                print(record)

def main():
    # create recommendation engine object
    re = RecommendationEngine("bolt://localhost:7687", "neo4j", "password")

    # clear the database
    re.clear_database()

    # insert nodes and relationships into Neo4j database
    re.insert_nodes_and_relationships()

    # recommend songs based on similarity between one given song and other songs
    # re.recommend_songs("The Call", "Regina Spektor", limit=5)

    # recommend songs based on similarity between multiple given songs and other songs
    re.recommend_from_multiple_songs(name_list=["The Call", "Two Birds", "Samson"],
                                     artist_list=["Regina Spektor", "Regina Spektor", "Regina Spektor"], 
                                     limit=5)

    # list all nodes in the graph
    # re.list_nodes()

    # get all relationships for a given song
    # re.get_relationships(0)

    # count number of relationships in the graph
    # re.count_relationships()

    # show the graph in Neo4j database
    # re.show_graph()

    # close the recommendation engine object
    re.close()

if __name__ == "__main__":
    main()