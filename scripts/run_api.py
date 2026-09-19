import uvicorn


def main() -> None:
    uvicorn.run(
        "city_bike.api:app",
        host="127.0.0.1",
        port=8000,
        reload=True,
    )


if __name__ == "__main__":
    main()
