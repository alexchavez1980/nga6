COPY 
staging.users_file(uri,name,image_url,followers_count,following_count,
	public_playlist,total_public_playlist_count,
	has_spotify_name,has_spotify_image,color,user_created_show)
FROM 'users_file.csv' DELIMITER ',' csv header;