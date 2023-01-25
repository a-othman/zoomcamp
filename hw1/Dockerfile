services:
  postgres:
    image: postgres:13
    environment:
      - POSTGRES_USER=root
      - POSTGRES_PASSWORD=root
      - POSTGRES_DB=ny_taxi
    volumes:
      - "/Users/ahmedothman/Desktop/dataengineering_zoomcamp/homework/hw1/ny_taxi_2019:/var/lib/postgresql/data"
    ports:
      - "5432:5432"
    image: dpage/pgadmin4
    environment:
      - PGADMIN_DEFAULT_EMAIL=admin@admin.com
      - PGADMIN_DEFAULT_PASSWORD=root
    ports:
      - "8080:80"