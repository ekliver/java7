package sys.bean;

import java.util.List;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;
import sys.dao.MovimientoAlmacenDAO;
import sys.imp.MovimientoAlmacenDAOImp;
import sys.model.AimarMovAlmacenCab;

@ManagedBean
@ViewScoped
public class MovimientoAlmacenBean {

    private AimarMovAlmacenCab movimientoAlmacen;
    private List<AimarMovAlmacenCab> listaMovimientosAlmacen;

    public MovimientoAlmacenBean() {
        movimientoAlmacen = new AimarMovAlmacenCab();
    }

    public AimarMovAlmacenCab getMovimientoAlmacen() {
        return movimientoAlmacen;
    }

    public void setMovimientoAlmacen(AimarMovAlmacenCab movimientoAlmacen) {
        this.movimientoAlmacen = movimientoAlmacen;
    }

    public List<AimarMovAlmacenCab> getListaMovimientosAlmacen() {
        MovimientoAlmacenDAO mADao = new MovimientoAlmacenDAOImp();
        listaMovimientosAlmacen = mADao.listarMovimientosAlmacen();

        return listaMovimientosAlmacen;
    }

    public void setListaMovimientosAlmacen(List<AimarMovAlmacenCab> listaMovimientosAlmacen) {
        this.listaMovimientosAlmacen = listaMovimientosAlmacen;
    }

}
