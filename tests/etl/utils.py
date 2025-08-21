from sqlmodel import Session, SQLModel, func, select


def get_row_count(session: Session, table: type[SQLModel]) -> int:
    return session.exec(select(func.count()).select_from(table)).one()
